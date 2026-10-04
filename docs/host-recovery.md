# Host startup and failure readback

The controller records a lifecycle receipt in its existing Store after acquiring the exclusive data-directory lock. Startup records `starting`; binding the IPC endpoint records `running`. Graceful shutdown drains admitted work before recording `stopped`. A failed supervisor or startup records `failed` with a bounded controller error code. Diagnostic receipts contain no native response body, credential or raw error message.

`host.status.host_lifecycle` returns the current receipt, the last exit and the latest failure. A later graceful exit updates `last_exit` but retains `latest_failure`; it does not acknowledge or erase the earlier failure. Failure history is factual, and does not block normal work. After a new host acquires the same exclusive lock, a previous `starting` or `running` marker produces `HOST_INTERRUPTED`. This proves that the earlier host did not record its exit; the cause remains unknown. The receipt belongs to the controller's host epoch and survives loss of a manager chat. A malformed optional lifecycle receipt produces a safe diagnostic without hiding the rest of host status. Database failures remain errors.

IPC connection failure before dispatch returns `HOST_UNAVAILABLE`. An incomplete authentication handshake returns `HOST_HANDSHAKE_FAILED`. Both mean that no application request was sent on that connection. An application request whose response was lost remains `OUTCOME_UNKNOWN`; reconnect and read its original Operation before deciding whether to retry. A host failure does not authorize replay of admitted native work.

If startup fails before the Store is available, the foreground launching process receives a terminal error naming `data_root`, `credential_bootstrap` or `store_start` and the controller error code. A database startup failure cannot promise a receipt in that unavailable database. While the host is down, IPC reports unavailability; lifecycle history becomes readable after a successful restart.

Owned OpenCode startup failures are persisted separately with their exact launch/open Operation and binding references. The current GM or local Operator receives a safe `manager_action_required` projection through `operation.get` and `swarm.exceptions.get`. The original Operation admission result remains intact. A replacement GM can inspect the same durable failure without the former chat.

After an owned service is observed, an error selecting its exact queued `agent.open` is retained separately as `runtime_dispatch_action_required` on the same read APIs. A deterministic opening-actor validation error rejects the queued operation before command admission. Ordinary idle and prerequisite waits remain waits. The diagnostic retains only the error code, stage and exact operation references, survives manager handover and service departure, and does not replace the launch admission receipt. `not_dispatched` describes the specific open command only when its retained state and absent send timestamp prove that boundary. A later send or native rejection keeps the outcome unknown. Readback never authorizes an automatic retry.

An OpenCode `agent.reconcile` whose retained target cannot be loaded records an
unknown outcome with the safe error code and `reconcile_target_load` stage in
its Operation result. It does not claim that native readback ran, settle the
reconcile request, or replay the original input. A completed readback can still
report `resolved: false`; that describes the actual read attempt and leaves the
original target unresolved.

`agent.state.observation.latest_native_failure` retains the latest safe native
connection or snapshot failure as `code` and `recorded_at_ms`. A later successful
connection check, snapshot, disconnect or host restart does not erase it. The
current `connection` and `native_transport_error` fields still describe current
transport status; historical failure does not block work or establish readiness.
Managers receive only the validated code and timestamp. Damaged optional history
produces `NATIVE_FAILURE_DIAGNOSTIC_CORRUPT` without a fabricated timestamp and
does not hide the rest of the binding state.

An OpenCode snapshot requires a validated root-session read. Its independent
optional read axes share a bounded deadline inside the existing whole-snapshot
budget. A slow configuration, family, request or child-log read produces a
partial observation with a safe axis failure; it does not erase completed axes.
Pending forms and permissions are cleared only by a successful validated read of
that session and request kind. Incomplete child-log reads retain prior terminal
evidence. One instruction-entry read supplies both configuration and goal
projections. A partial observation does not establish family completion or
settle an unknown native input.

Managers can inspect these current partial-read diagnoses at
`agent.state.observation.native.failures` (also through `agent.list`). Each item
contains a bounded safe `code`, an allowlisted `source` and a validated optional
`session_id`; native bodies, paths, messages and credentials are omitted. At most
64 entries are returned, with the full `failure_count` and `failures_truncated`
flag when the stored list is valid. `gaps` remains the accumulated observation
gap counter. Malformed diagnostic data produces
`NATIVE_SNAPSHOT_DIAGNOSTIC_CORRUPT` without fabricated counts. These facts are
current snapshot coverage; `latest_native_failure` is separate durable history.

The OpenCode supervisor inspects completed worker handles. A panic, cancellation
or unexpected return while the exact binding/service scope remains active
records a fixed safe native worker failure and moves already sending or
native-accepted work to unknown before a replacement worker can start. Released
scopes and normal host shutdown do not produce false crash receipts. Failure to
persist recovery is a supervisor error propagated to the host lifecycle. The
existing per-scope supervisor recreation does not replay native input. Snapshot
readiness restoration also retains recovery and unresolved-input guards.

Implementation qualification is recorded in `docs/implementation-status.md`. These receipts do not introduce an automatic host restart, a model call or a new daemon.
