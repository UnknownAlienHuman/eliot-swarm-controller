# OpenCode route — standalone Rust consolidation

## Current decision

Keep `crates/swarm-adapter-opencode` as the only target executor for new OpenCode
work. The current host-integrated `runtime/opencode_v2` route remains active only
until the standalone route closes the exact result and restart boundaries below.
After cutover, delete the built-in executor and its worker/demand/package surface.

Do not keep both implementations as permanent profiles. Historical completed records
may remain readable through Store projections; they do not keep an executor alive.

## Native contract selection

The prospective adapter is currently pinned to an older OpenCode projection whose
assistant message omitted the source input parent. It therefore fails every assistant
result read with `NATIVE_ASSISTANT_PARENT_UNAVAILABLE`. This fail-closed behavior is
correct for that contract, but it is not a permanent product limitation.

Current upstream source reviewed at
`930c0c8b6a3c41ceb73b9372f878b5a1186b4d4e` exposes the mechanisms Eliot needs:

- durable session inbox/message identity;
- one process-global execution owner per Session ID;
- first-admission-wins input identity and coalesced wakeups;
- a write-ahead execution claim and exact restart interruption settlement;
- public assistant messages with `parentID`;
- indexed session/message/inbox sequences.

This source review is **not** an installed-version qualification. Select one exact
installed OpenCode contract that exposes the required fields, record its actual
version/source coordinates, and qualify it before admitting new work. Do not add a
2.0.7/newer compatibility matrix or guess support from semver.

## Ownership

The OpenCode service is externally owned. Eliot observes and controls sessions through
the selected native API but does not start, update, restart or kill the shared service.
A service PID change is not permission to adopt foreign state.

The standalone adapter owns:

- one selected service/store identity;
- one ordered native owner per Session ID;
- Operation → durable inbox/input identity;
- native reply/steer/cancel/background/readback;
- exact result association;
- provider-local observation and usage folding;
- already-formed Store delivery until ACK.

Host Store owns Tasks, Attempts, Operations, authorization, workspace authority,
review and acceptance.

## Semantic actions

The selected contract may expose:

```text
Open
Dispatch
Send(next_turn | exact steer)
Reply
Background
Cancel(turn | queued_input)
Refresh
Result
Reconcile
```

Live Configure is advertised only when the selected native contract supplies an exact
setter and matching readback. OpenCode has no reviewed native Harness Goal in the
selected upstream source; Eliot Goal therefore sends ordinary `Send` messages.

`native.opencode.loop_step` is not a product action. Public `native.mcp.*` setup phases
are adapter internals. External service shutdown is not an Eliot action.

## Input and result identity

One Eliot Operation creates one logical native input ID. OpenCode may perform several
physical Attempts inside that input; they remain observation, not new Operations.

```text
Operation
  -> durable inbox/input ID
  -> process-global Session execution owner
  -> exact assistant message ID
  -> assistant.parentID == source input ID
  -> exact selected part IDs/content
  -> module.result
```

The result selector contains the exact assistant ID, expected parent input ID and
selected part IDs/revision. The adapter reads that exact message and rejects:

- missing or changed parent;
- assistant from another input/session;
- changed selected part set or revision;
- newest-message/order/timestamp inference;
- an unclassified stream/log gap.

The old pinned contract continues to return `NATIVE_ASSISTANT_PARENT_UNAVAILABLE` for
historical readback where the edge does not exist. New work moves to the single
selected current contract rather than preserving two result implementations.

## Restart and uncertainty

A Host/adapter reconnect never replays native input. The native durable inbox and
execution claim are the source of truth.

The reviewed upstream restart path records an interrupted terminal for an execution
claim left by the prior process. The selected installed service must prove one of:

1. exact continuation linked to the same input;
2. exact final interruption linked to that input.

If neither is available, the Eliot Operation remains `outcome_unknown` and keeps its
resource fence. Idle state, list absence, a new PID or elapsed time never settles it.
Issue #3 remains the owner-machine qualification boundary for service restart.

## Local observation

The adapter reads provider-local state without a provider/model call:

- known service/project/store identity;
- session, message and inbox sequences;
- execution claim/terminal state;
- pending permission/form requests;
- child/background sessions where the native source exposes them;
- local usage/provider facts.

Use one read-only WAL-visible SQLite connection per selected store and bounded indexed
queries. Preserve source epoch/cursor and revisit known open messages that can update
in place. `PRAGMA data_version` is only an unchanged-pass hint on the same connection.
Do not use `immutable=1` on a live DB, copy the DB without WAL, or open a connection per
viewer.

Unmatched sessions remain visible inventory and grant no control rights. Issue #4
remains the live child-discovery qualification boundary.

## MCP boundary

OpenCode MCP plugin installation/discovery/readiness belongs inside this adapter. It
reports asynchronous observation and cannot gate the first assigned input. Remove
Store/public `native.mcp.install|observe|arm|read` methods and their proof packets when
the standalone route takes ownership.

The Eliot MCP frontend remains independent and continues to call ordinary Store
methods through `rmcp`.

## Persistence simplification

The current standalone journal combines native intent, execution periods, outcomes,
result selectors/pages, Store ACKs, references, checkpoints, tombstones, cleanup and
retention. Replace it with four bounded owners:

```text
SessionRecord
  selected service/store/session identity and current source epoch

NativeEffectRecord
  one unresolved Operation/input plus native readback coordinates

StoreDelivery
  exact already-formed outcome/result parameters until ACK

ObservationCheckpoint
  replaceable latest normalized state plus cursor/coverage
```

Do not create a universal event journal, second transcript database, reference graph,
tombstone/compaction system or another scheduler.

## Cutover sequence

```text
Q0  select and live-qualify one exact native contract with durable inbox,
    execution claim/restart terminal and assistant parentID

Q1  update standalone native DTOs/reads; bind result by exact assistant ID + parentID

Q2  map exact steer/reply/cancel/background and restart readback;
    unsupported controls fail before native I/O

Q3  move MCP setup/readiness fully inside the adapter and remove first-input gates

Q4  switch current route/config/package selection to standalone Rust;
    admit no new work through built-in host executor

Q5  disposition concrete built-in unsettled work; delete runtime/opencode_v2,
    legacy_worker_demand/OpenCode optional-worker slot, built-in package profiles,
    launcher native-MCP/owned-service paths and built-in-only fixtures

Q6  shrink the standalone journal to SessionRecord, NativeEffectRecord,
    StoreDelivery and ObservationCheckpoint; remove old pinned profiles/readers after
    required result export
```

Preserve generic Store Operation/result/artifact/observation APIs while removing
OpenCode-specific host execution.

## Verification

Focused connected cases:

- same input ID admits once; changed same-ID request conflicts;
- repeated wake joins/coalesces rather than starting a second owner;
- exact assistant result verifies `parentID` and selected parts;
- old no-parent result fails closed without guessing;
- graceful/forced service restart yields exact continuation/interruption or honest
  unknown, never prompt replay;
- queued cancel differs from active turn cancel;
- reply addresses one current pending request;
- MCP unavailability does not block first dispatch;
- unbound sessions remain locally observable;
- after cutover no config, package, worker demand, fallback or fixture starts the
  built-in executor.

Historical receipts qualify only their named contract and do not qualify the selected
current native service.