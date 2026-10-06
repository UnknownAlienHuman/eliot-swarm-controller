# Host startup and failure readback

The controller records a lifecycle receipt in its existing Store after acquiring the exclusive data-directory lock. Startup records `starting`; binding the IPC endpoint records `running`. Graceful shutdown drains admitted work before recording `stopped`. A failed supervisor or startup records `failed` with a bounded controller error code. Diagnostic receipts contain no native response body, credential or raw error message.

`host.status.host_lifecycle` returns the current receipt, the last exit and the latest failure. A later graceful exit updates `last_exit` but retains `latest_failure`; it does not acknowledge or erase the earlier failure. Failure history is factual, and does not block normal work. After a new host acquires the same exclusive lock, a previous `starting` or `running` marker produces `HOST_INTERRUPTED`. This proves that the earlier host did not record its exit; the cause remains unknown. The receipt belongs to the controller's host epoch and survives loss of a manager chat. A malformed optional lifecycle receipt produces a safe diagnostic without hiding the rest of host status. Database failures remain errors.

IPC connection failure before dispatch returns `HOST_UNAVAILABLE`. An incomplete authentication handshake returns `HOST_HANDSHAKE_FAILED`. Both mean that no application request was sent on that connection. An application request whose response was lost remains `OUTCOME_UNKNOWN`; reconnect and read its original Operation before deciding whether to retry. A host failure does not authorize replay of admitted native work.

If startup fails before the Store is available, the foreground launching process receives a terminal error naming `data_root`, `credential_bootstrap` or `store_start` and the controller error code. A database startup failure cannot promise a receipt in that unavailable database. While the host is down, IPC reports unavailability; lifecycle history becomes readable after a successful restart.

Owned OpenCode startup failures are persisted separately with their exact launch/open Operation and binding references. The current GM or local Operator receives a safe `manager_action_required` projection through `operation.get` and `swarm.exceptions.get`. The original Operation admission result remains intact. A replacement GM can inspect the same durable failure without the former chat.

Read-only technical facts and action authority are separate. When safe
technical facts are retained on an Operation, an authenticated Manager or
local Operator who already passes the ordinary `operation.get` visibility
check can read those facts on that exact Operation without the originating
chat, a live native session or producer, current-GM/lease status, or a
still-current Task/Attempt. When present, this covers the retained
`native_mcp_readback`, `native_mcp_tools_readback`, `workspace_failure_readback`
and `participant_issuance` diagnostic fields. They remain attached to the
exact retained operation and any project, binding, generation or session
evidence it already carries; this does not widen visibility to unrelated
projects or give Participants cross-Task access.
Current-GM or local-Operator action suggestions remain distinct, and every new
command or effect still passes its current action-authority checks. Reading a
historical fact never authorizes retry, replay or mutation.

After an owned service is observed, an error selecting its exact queued `agent.open` is retained separately as `runtime_dispatch_action_required` on the same read APIs. A deterministic opening-actor validation error rejects the queued operation before command admission. Ordinary idle and prerequisite waits remain waits. The diagnostic retains only the error code, stage and exact operation references, survives manager handover and service departure, and does not replace the launch admission receipt. `not_dispatched` describes the specific open command only when its retained state and absent send timestamp prove that boundary. A later send or native rejection keeps the outcome unknown. Readback never authorizes an automatic retry.

An OpenCode `agent.reconcile` whose retained target cannot be loaded records an
unknown outcome with the safe error code and `reconcile_target_load` stage in
its Operation result. It does not claim that native readback ran, settle the
reconcile request, or replay the original input. A completed readback can still
report `resolved: false`; that describes the actual read attempt and leaves the
original target unresolved.

For supported Command, Codex-controller and Antigravity bindings, a verified
module-owner departure followed by a different bridge boot marks in-flight
Operations unknown. Current-GM or local-Operator `operation.get` exposes
`module_recovery_action_required` for the exact Operation and binding
generation, including the verified old/new boot IDs and an adapter-supported
`agent.reconcile` template. A successor GM receives the same action without
rewriting historical actors. The cause and native effect stay unknown;
`retry_authorized` and `native_replay` are false. Command `.3`/`.4` reconciliation
targets only `agent.open` or `task.dispatch`; Codex-controller `.3` and
Antigravity `.2` also accept `agent.send`. Missing prior-boot evidence can leave
the original Operation unknown, especially Antigravity's process-local journal.
No native input is replayed by this projection.

`agent.state.observation.latest_native_failure` retains the latest safe native
connection or snapshot failure as `code` and `recorded_at_ms`. A later successful
connection check, snapshot, disconnect or host restart does not erase it. The
current `connection` and `native_transport_error` fields still describe current
transport status; historical failure does not block work or establish readiness.
Managers receive only the validated code and timestamp. Damaged optional history
produces `NATIVE_FAILURE_DIAGNOSTIC_CORRUPT` without a fabricated timestamp and
does not hide the rest of the binding state.

The current source also retains native MCP readback failures in the launch
manifest's sibling `native_mcp_latest_failure` record. It exposes only the safe
error code, closed stage, timestamp and category; current-GM
`operation.get` includes `native_mcp_readback.latest_failure` after handover.
The producer and current-manager readback have passed the focused source gate,
as recorded in `implementation-status.md`; fresh native qualification is pending.
The receipt is failure history, not native MCP
proof, dispatch authority or retry permission. Reading it does not authorize
replaying a `task.dispatch` or repeating an uncertain input.

The qualified 251dd55 source also retains participant-issuance failures on the
exact parent launch as `participant_issuance_latest_failure`. Current-manager
`operation.get` exposes the safe code, closed stage, timestamp and category at
`participant_issuance.latest_failure`, including the preparation,
credential-issue and commit stages. Database persistence and selector failures
propagate to the host instead of being reported as an issuance diagnostic. The
receipt reports an issuance failure and does not authorize Task input replay.

C23's route mismatch was repaired in qualified source 22420c8: owned identity
and PID now come from the verified projection for the exact binding generation.
C25 exercised that route but ended with outer `service_start` timeout and
`OWNED_SERVICE_OR_NATIVE_PROOF_TIMEOUT`. Its C7 owned-service readback was
retained twice; 23 runtime snapshots succeeded with zero gaps or axis failures,
and four current-manager C7 readbacks validated. C8's actual retained failure
was `NATIVE_MCP_PROOF_SOURCE` at `challenge_preflight`, before effect
reservation. The participant was installed, registered and connected and the
challenge was prepared, but no effect was reserved and tool readback was null.
The exact failed predicate remains unproven because the source merged multiple
guards and did not retain the native response. At the C25 source, C8 failure was not exposed
by current-manager `operation.get` or `swarm.exceptions.get`; native MCP proof
and model execution remain unqualified. Preserve the consumed C25 run without
replay.

The source-qualified increment adds the bounded `native_mcp_tools_readback` operation
projection, precise closed source-guard codes, and a readback-only action for
admitted launches whose unknown start predates a validated interruption and the
current host start. Formatting, strict production Clippy, seven distinct Manager
regressions, build, full Windows/Ubuntu CI and independent review passed as
recorded in `implementation-status.md`. The action preserves exact binding
references and current-GM authority independently of the previous chat. It
reports an unresolved reservation and does not establish which process started,
create a terminal observation or authorize retry. C8 diagnostics include
scheduler-only safe codes; corrupt optional metadata produces a safe gap without
hiding the original Operation.

C26's manager-readback harness ended in phase
`native_mcp_tools_manager_readback` with `NATIVE_MCP_TOOLS_MANAGER_READBACK_REQUIRED`
after classifying one public C8 readback as
`MANAGER_NATIVE_MCP_TOOLS_PROJECTION_MISMATCH` (zero valid, one failed). Three
independent read-only reviews found a harness alias collision: schema names
`assignment_type`, `last_error_type` and `challenge_type` generated `*_type_type`
JSON type aliases, while the harness inspected extracted object/null values.
The underlying C8 state was valid/running with no persisted error and no effect
reserved. C26 did not validate the public projection and established no
production source-guard diagnosis. The corrected C27 parser did not reproduce
the corruption/private-read error. Preserve the consumed C26 claim without
replay.

C27 run `b967289b-c085-48f0-9464-d882b2a1213c` ended at
`native_mcp_tools_manager_readback` (`exec1819`, exit 1). The exact retained C8
failure was `NATIVE_MCP_PROOF_PLUGIN_MISSING` at `challenge_preflight`, recorded
at `1791143831520`, after open at `1791143730468` and first-ready at
`1791143749914`; no challenge effect was reserved. The corrected harness
validated the exact current-Manager C8 readback (1 valid, 0 failed), C7
readbacks (2 valid, 0 failed), and six successful runtime snapshots (zero
failures or gaps). This proves current-Manager error delivery for this exact
failure, but not native MCP proof or model execution. A subsequent bounded
offline probe of the pinned loader proved that the Windows verbatim package
path resolves no server entrypoint, while an ordinary absolute spelling of the
same canonical directory loads the expected plugin. The correction changes
only the serialized configuration path and its exact comparison consumers;
canonical file, digest and scope validation remain intact. Offline loading does
not establish service activation or native MCP proof; the fresh source gate
and native run are recorded in `implementation-status.md`.
Preserve the consumed C27 claim without replay. C24 hosted Bunny remains private
preparation until full native MCP proof exists.

C28 used the qualified Windows configuration-path correction and ended with
`NATIVE_MCP_PROOF_PLUGIN_STATE_MISMATCH` at `challenge_preflight`. The current
Manager again read the exact persisted code, stage and timestamp successfully
(one validated C8 readback, zero failed or corrupt readbacks). The expected
plugin ID existed and passed uniqueness validation; its active server state
failed validation. No challenge effect was reserved. This error delivery is
qualified for that exact failure and does not prove plugin activation, callable
tools or model execution. Preserve the consumed C28 claim without replay.

The activation correction resolves the internal MCP service only inside the
guarded `arm` RPC handler, where the pinned request location provides it, rather
than during plugin activation. A controlled offline regression verifies bare
activation, scoped native readback and rejection/deduplication before extra MCP
reads. C29 then passed plugin identity/active-server/source preflight but retained
`NATIVE_REJECTED` at `challenge` after reserving the arm effect. Subsequent
read-only recovery retained `NATIVE_REJECTED` at `tools_readback`. The outer
startup timeout does not explain the RPC rejection, and the raw rejection class
was not retained. C29's harness validated four C7 Manager readbacks but did not
read this post-reservation failure through the Manager C8 projection. Neither
native MCP proof nor model consumption is qualified. Preserve the consumed C29
claim and uncertain arm without replay; further diagnosis does not authorize
resending either.

The subsequent RPC source repair uses schemas accepted by the installed
OpenCode decoder while retaining exact handler validation. New arm/read HTTP
rejections retain only a closed safe `rejection_class`, alongside stage, code
and time, through the current-Manager Operation projection. A successor Manager
can read the same receipt; no original chat identity is required. Raw HTTP
bodies, native messages and credentials are not projected.

C30's consumed run `9bec4e52-d5c2-45f2-aabd-a3f857b7aa1c` stopped at the
harness inventory predicate. Its retained inventory contains nine observed
tools and a valid digest; hook observation sequence zero is valid when both
model-dependent hook statuses are unknown. This is a qualification-harness
failure, not proof of a rejected native inventory. Preserve that run and claim
without replay. C31 is a fresh corrected preparation; full lifecycle and model
execution require their own run evidence.

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

## Optional module-supervisor child recovery

Before child `exec`, the coordinator writes and reads back
`MODULE_SUPERVISOR_SPAWN_PENDING`. It does not spawn unless that durable intent
and prior-child departure evidence are confirmed. A restart finding the pending
marker without a child receipt keeps the launch fenced.

The prior child is resolved only by a retained confirmed-exit receipt or exact
identity reconciliation using PID, process-birth identity and image. A matching
live child remains owned; uncertain identity keeps a safe Manager error in
`host.status.host_lifecycle.optional_workers["module-supervisor"].last_error_code`.
This retained health read uses ordinary authorization and is independent of any
particular GM or chat; it does not authorize a launch. A detached one-shot
reaper waits for the exact verified child only while the runtime exists. If the
runtime is gone, it sends no signal and claims no departure; the handle remains
unreaped and durable pending/uncertain state awaits Manager recovery. This is a
source boundary, not a runtime-qualification claim.
