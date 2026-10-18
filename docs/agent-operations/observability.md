# Observability — Adjustable Logs and Live Agent Monitoring

Observation explains current work and failures without starting model calls.
[Implementation](implementation.md) owns the remaining source changes.
At `3b910fa`, [swarm-telemetry](../../crates/swarm-telemetry/src/lib.rs) and
[swarm-observer](../../crates/swarm-observer/src/host.rs) use a bounded
Producer/HostRecorder chain. Tracing replacement and broader read-pool routing are
targets, not implemented guarantees.

## 1. Three data classes, one explanation of reality

| Data | Existing authority and lifetime |
| --- | --- |
| Business facts | Store Operations, observations and artifact references. Commit before acknowledging an action; log settings cannot disable these records. |
| Diagnostic records | Optional bounded Producer/recorder output. Loss is counted; a log line is not an action receipt. |
| Live view | Read-only projection of retained facts and real process/native observations, with explicit freshness and gaps. |

Keep large output once under its actual owner. A viewer does not maintain another
conversation or Task store. Silence, disconnection, idle and completion differ;
none requires asking the model for a heartbeat.

### Host terminal events and retained diagnostics

[Host lifecycle](../../crates/swarm-kernel-host/src/store/host_lifecycle.rs) retains
exit/failure receipts and their diagnostic codes. Normalized `host.exit` and
`host.failed` can be views of the same host-epoch occurrence. Their shared safe
selector payload is not an extensible copy of the diagnostic receipt. Preserve
one occurrence when routing a selected action; changing a producer does not replay
historical events or repair already-consumed malformed pairs.

### OpenCode catalog rejection receipt

For the exact rejected `agent.open`, `operation.get` can retain a bounded
`NATIVE_MODEL_UNAVAILABLE` category: catalog unavailable, model absent/disabled
or variant absent. This existing [result projection](../../crates/swarm-kernel-host/src/store/operations/execution_diagnostic.rs)
exposes neither raw catalog/error text nor credentials. It is a diagnostic,
not readiness or permission to retry.

## 2. Configurable depth without a restart

Level and content are separate choices. Current levels are
`off/error/warn/info/debug/trace`; metadata is the default content.
`redacted_text` is supported for selected module-supervisor diagnostics.
Native frames are unsupported: no bounded frame producer exists. Trace cannot
silently enable prompts, tool arguments, environment dumps or secrets.

The current operator live-policy format and diagnostic wire format are different
contracts. Their existing parsers own version handling. Use one current format
after the [finite transition](implementation.md#current-format-and-existing-data);
do not maintain version tables or add old-format readers to every producer.

### Host recorder configuration

The optional recorder is disabled by default. Current TOML fields are:

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

Optional `directory` defaults to `<storage.data_dir>/diagnostics`; relative paths
resolve against the controller config. These are bounded engineering defaults,
not measured fleet sizing. Level/content belong in the separate live-policy file.
The existing recorder loads it lazily; disabling the recorder creates no permanent
idle recorder thread.

### Operator live policy

The current schema-3 JSON file names the canonical local data root as `scope_id`.
For example:

```json
{
  "schema_version": 3,
  "config_version": 1,
  "scope_id": "C:\\path\\to\\canonical-data-root",
  "level": "info",
  "content": "metadata",
  "included_kinds": [
    "client_disconnected", "store_operation_failed", "module_started",
    "module_stopped", "agent_delivery_failed", "recorder_failure"
  ],
  "overrides": [],
  "retention_bytes": 134217728,
  "retention_days": 7
}
```

An override selects one module, client or Operation and has absolute expiry.
The existing [live-config parser](../../crates/swarm-observer/src/live_config.rs)
caps file size, overrides and selector lengths. Explicit operator `off`,
excluded kinds and matching metadata-only restrictions remain effective.
Validate a whole change before swapping its filter; invalid input must not enable
content capture. This local reload is not a fleet-wide revision/ACK protocol.

### Manager tool

`logging.get/set` lets a registered Manager inspect and change its own scoped
diagnostic policy. The MCP names are `logging_get` and `logging_set`; arguments
are flat. Omitting selectors targets the authenticated Manager's client:

```json
{
  "client_request_id": "diagnostic-manager-debug-001",
  "level": "debug",
  "content": "redacted_text",
  "ttl_seconds": 600
}
```

Task scope requires its complete Task/revision/Attempt tuple; binding scope requires
ID and generation. Current policy TTL is 1–86,400 seconds. Store commits policy and
reloads the existing Producer; expiry is checked during emission and after restart.
Manager scope cannot expose another principal or override operator restrictions.

`logging.get` distinguishes source capability, loaded policy and runtime
availability. A passing non-emitting probe is not proof that a recorder wrote a
file. Saving policy starts no native session. Source/status gaps stay visible.

## 3. Common record and causal relationships

Use the existing small diagnostic envelope: kind, severity, phase, observed time,
bounded code/component and known scope IDs. The actual
[`WireRecord`](../../crates/swarm-telemetry/src/lib.rs) already carries optional
module, binding/boot, Task/Attempt and Operation correlation. Its sequence counts
emission attempts, including loss; it is not a Store journal cursor or a total
arrival order.

Add fields only where a producer observes them and a consumer needs them.
Native turn, process exit, elapsed time and review outcome belong to their source
records; they do not justify a universal event DTO with dozens of nullable fields.
Use existing IDs/references to join views. Trace/span context is optional correlation,
not authority. Compare durations only within one monotonic clock domain.

Requested model/effort differs from observed effective model and usage. Missing
usage remains unknown, not zero. Do not reconstruct unavailable reasoning or native
identities. Show which work failed, the observed phase/outcome, its source freshness
and the supported recovery action; retain original actors across handover.

## 4. Implementation and failure isolation

Replace the existing logging plumbing through its callers using `tracing`,
`tracing-subscriber` filtering/reload and the selected recorder sink. Remove the
old path as callers move. No second logging framework, control service or universal
telemetry schema is needed.

The reviewed [tracing-appender API](https://docs.rs/tracing-appender/0.2.5/tracing_appender/non_blocking/struct.NonBlockingBuilder.html)
bounds queued lines, not total bytes. Preserve per-record and aggregate byte limits,
loss accounting and the worker guard in the same sink. Check enabled level/content
before expensive formatting. Graceful flush is bounded; power loss can lose trailing
diagnostics. Do not claim exactly-once logs.

Keep protocol stdout separate. Redact bounded logical records before serialization,
persistence and fanout, including secrets split across native chunks. Existing Atlas
calls are stateless, so a chunk buffer must establish that boundary. Suppress content
whose safe bounds cannot be established. Escape terminal control sequences when
rendering and keep files scoped to private controller state.

One upstream reader fans out through bounded queues. A slow viewer cannot block
control, cancellation or terminal evidence. Recorder/stderr failures have separate
bounded error paths; recording an error must not recurse through its broken sink.
Store journal failure remains a failed/unknown admission, never acknowledged log loss.

Rotate diagnostics by byte/age limits and expose disk exhaustion and lost intervals.
Their retention must not remove evidence referenced by unresolved work. Diagnostic
sampling cannot drop business receipts. Keep actual process/native ownership outside
the recorder.

## 5. Live monitoring and future charts

The target initial snapshot uses a short authorized read transaction with its
journal cut, followed by events after that cursor. Apply the existing
[Store read conversion](implementation.md#store-and-current-data); ordinary reads
are not all on a read pool today. A lagged subscriber replays retained facts;
an expired cursor reports a gap and obtains a fresh snapshot.

Show current manager/agent work, module state, last activity, pending reason,
queue pressure, outcome and recovery action. A viewer disconnect cannot stop the
agent. Share process/resource observations; do not spawn a poller or model call
for each viewer. Do not count one shared process's RSS once per hosted session.

Metrics use bounded dimensions such as runtime/status. Task and Operation IDs
belong in logs, not unlimited metric labels. Add a bounded export or chart only
for an actual consumer; OTLP, Prometheus and dashboards are optional integrations.

## 6. Privacy and acceptance

Check principal/object scope before reads, subscription and artifact pagination.
Validate only the observation boundary changed: relevant filter reload/expiry,
disabled-recorder behavior, bounded loss/slow-reader gaps, split-secret redaction
or recorder failure. Ordinary agent work must proceed with observation absent.
These checks do not start a separate fleet/native qualification program.
