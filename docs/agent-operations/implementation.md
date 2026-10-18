# Eliot Swarm Controller — Implementation

This is the single implementation program for the
[architecture](architecture.md). Source was rechecked at
`e7906bfba989d40526bca8a39ecc5d278adf9448` on 2026-10-18. Source descriptions
below name existing code; instructions describe the target unless explicitly marked
completed. [Findings](refactoring.md) preserve the evidence and measured surfaces;
[status](../implementation-status.md) records completed changes and runtime receipts.

Do not add another roadmap, protocol family or compatibility registry. Complete a
connected slice from Store admission through the selected adapter and result/readback,
then delete the replaced path.

## 1. Ownership

| Responsibility | Owner |
| --- | --- |
| Tasks, Attempts, Operations, rights, reservations, acceptance and durable observations | [Host Store](../../crates/swarm-kernel-host/src/store/mod.rs). `swarm-store` owns SQLite connection/transaction mechanics, not domain policy. |
| Shared semantic adapter protocol | [swarm-contracts](../../crates/swarm-contracts/src/lib.rs). It contains the current command/event types and bounds, with no vendor structs. |
| Authenticated Eliot IPC and common adapter loop | [swarm-client](../../crates/swarm-client/src/lib.rs) and its existing `ModuleLink`. |
| Process ownership and adapter bootstrap identity | [swarm-process](../../crates/swarm-process/src/module_owner.rs). |
| Vendor transport, native session state, native recovery and local provider readers | The selected `swarm-adapter-*` implementation. |
| Process/worktree/Git collection and joined public projection | Existing host lifecycle and Store read owners; optional diagnostics remain in `swarm-telemetry`/`swarm-observer`. |
| Scheduling enabled helpers | One `swarm-automation` service with due state committed by Store. |
| CLI and MCP | `swarm-cli` and `swarm-mcp`, both calling the same Store methods. |

No adapter stores Tasks or accepts candidates. No frontend interprets vendor events.
No common helper owns vendor retry or reconnect policy.

## 2. Implementation order

### P1 — Replace the module compatibility matrix with protocol 2

Edit:

- [module_command.rs](../../crates/swarm-contracts/src/module_command.rs)
- [runtime.rs](../../crates/swarm-contracts/src/runtime.rs)
- [module_contract.rs](../../crates/swarm-contracts/src/module_contract.rs)
- [module_catalog.rs](../../crates/swarm-contracts/src/module_catalog.rs)
- [module_handshake.rs](../../crates/swarm-kernel-host/src/store/module_handshake.rs)
- Store command admission and public method catalog.

Introduce one typed internal `AdapterCommand` protocol. Public Store methods can keep
stable names, but the adapter does not receive arbitrary method strings or vendor
RPC names.

```text
Open          open/resume/attach a selected native session
Dispatch      initial TaskPrompt for one Task/Attempt/Operation
Send          next-turn or exact active-turn steering input
Reply         answer one observed pending request
Configure     apply supported model/mode/options
Goal          set/edit/pause/resume/clear native Harness Goal
Background    move an eligible foreground tool/run to native background
Cancel        cancel one exact turn or queued input
Refresh       explicit safe native refresh/readback
Result        retrieve output attributable to one admitted input
Recover       re-establish one known native session after confirmed owner/connection loss
Reconcile     read back one uncertain admitted action
Shutdown      release or stop an explicitly owned native resource
```

Use enums/closed payloads for common fields. A vendor-only action remains private to
its adapter until it has a real controller consumer. Do not expose a generic
`native.*` execute method.

Remove from the common command set and Store public routing:

- `native.opencode.loop_step`: it is `Send { delivery: steer }` where supported;
- `native.command.cancel_turn`: it is `Cancel { target: turn }`;
- `native.command.close_session`: it is `Shutdown`/release;
- `native.command.release_evidence`: internal cleanup, not manager control;
- `native.mcp.install|observe|arm|read`: adapter-internal setup/observation.

Keep `Recover` as a semantic action. It re-establishes a known session/binding after
confirmed owner or control-connection loss and never replays the Task input. It is
distinct from `Open` (select/open/attach) and `Reconcile` (read back an uncertain
effect). Adapters that can recover declare it; unsupported adapters fail explicitly.

Keep `Background` as an optional semantic action because a real OpenCode operation
uses it. Unsupported actions fail before native I/O.

Define one compact `AdapterCapabilities` value:

- supported actions;
- `next_turn` and/or exact `steer` delivery;
- cancel scopes (`turn`, `queued_input`);
- supported native Goal actions;
- optional background control;
- local activity, result and account-usage source availability.

Capabilities are descriptive routing data. Store rights are checked separately.
Do not encode artifact versions, schema permutations or package history in this
value.

### P2 — Simplify descriptor and hello

The installed descriptor remains package metadata: module ID, executable, launch
configuration, process lifecycle, current protocol major, and required semantic
actions. Installation can verify selected executable/package bytes once.

At process start:

1. `swarm-process::module_owner` verifies the current process belongs to the exact
   supervisor-owned scope and loads binding, generation, client and credential
   identity.
2. The adapter reports protocol major and its compiled `AdapterCapabilities`.
3. Store checks that the descriptor-selected requirements are a subset of the
   compiled handlers and returns the selected capability subset.
4. `ModuleLink` marks the authenticated link ready.

Delete the current full echo/compare of module ID, artifact version/build, config
schema and command/event schema arrays on both sides. The supervisor already selected
the package; repeating its entire descriptor in every process does not authorize an
effect. Keep the protocol major and current package identity in diagnostic/inventory
facts, not as branches in Store behavior.

Delete:

- `ModuleContractTemplate::codex_rust_controller_v1..vN` ladders;
- `CODEX_RUST_CONTROLLER_CURRENT_DESCRIPTOR_VERSION` as a Core behavior selector;
- hardcoded OpenCode/Command artifact-version checks in
  `selected_native_command_supported`;
- legacy selectorless module behavior after the finite transition;
- exact capability/schema-set matchers copied into every adapter and JS bridge;
- command/event schema inventories when protocol 2 types replace them.

Configuration is validated once by the selected adapter's current typed config at
install/start. Do not negotiate the config schema on every hello. A package version
can remain visible for diagnostics and rollback; it cannot decide whether a common
command is semantically valid.

### P3 — Build the small shared Rust adapter shell

Do not create a new crate. Extend existing owners only after two current Rust
adapters use the same shape.

`swarm-process::module_owner` gains one common bootstrap reader that returns:

- verified owner/worker membership;
- binding ID and generation;
- module client ID and credential path;
- private state directory;
- selected protocol and capability requirements;
- typed host connection config path.

It does not parse vendor configuration or launch native processes.

`swarm-client` gains a small `adapter` module around `ModuleLink` that owns:

- connect and one hello per physical link;
- reconnect of the Eliot link;
- `module.next` pulling;
- closed `AdapterCommand` decoding and common envelope checks;
- exact re-delivery of already-retained `module.outcome`, `module.result` and
  `module.observe` records until Store acknowledges them;
- bounded shutdown of the adapter loop.

The shell accepts callbacks/trait methods only for:

- execute one typed native action;
- return pending retained Eliot deliveries;
- acknowledge a retained delivery;
- release or stop the selected native scope according to actual ownership.

It does **not** own the vendor session, native command queue, result association,
uncertain-effect readback, Task state, timers or scheduling. Those stay in the
adapter and Store.

Convert standalone OpenCode and Codex as the two reference consumers in the same
slice. Delete their repeated env parsing, claim equality, hello response validation
and outer delivery loops. Then migrate Muse, Claude, Command ACP and Antigravity one
at a time, deleting the corresponding duplicated Rust/JS bootstrap and obsolete
artifact profiles. Do not add the shell first and leave wrappers around old loops.

### P4 — Simplify receipts and remove proof packets

The authenticated link, binding/generation and retained Operation already identify
who may report an outcome. Protocol 2 outcome identity needs only:

- Operation ID;
- binding generation/adapter boot needed to reject stale reports;
- action disposition and optional native input/turn IDs;
- action-specific bounded details.

Store loads the original Operation and validates that it belongs to the authenticated
link and current/historical permitted generation. The adapter does not echo module
ID, artifact version/build, protocol and canonical request hash in every outcome.
A local adapter journal may keep an internal digest to reject conflicting replay of
the same Operation; this digest is not another Store contract.

Delete or collapse:

- `ModuleReceiptIdentity`'s repeated package/protocol/request identity;
- duplicated Task/Attempt/revision and source-text hashes across
  `TaskDispatchContext`, admission receipt and result-origin structures;
- Goal continuation admission/receipt/evidence protocols once Eliot Goal sends an
  ordinary `Send` Operation;
- `AntigravityDispatchSessionProof` and other vendor readiness proof structs from
  `swarm-contracts`;
- native MCP protected command envelopes from the public runtime protocol.

Keep hashes only for actual immutable bytes that cross a storage boundary:
credentials/verifiers, selected executable/package verification at install/launch,
and result/artifact content. A result page retains payload range, length and content
hash because the bytes are stored and assembled. Native state/model/MCP readiness is
an observation with IDs and revisions, not a cryptographic admission token.

### P5 — Convert native execution and remove duplicates

#### OpenCode

Use the standalone Rust adapter as the only new-work executor. Map:

- `Open` to selected session create/resume;
- `Dispatch` and `Send` to the native inbox with `next_turn` or steer semantics;
- `Reply` to the exact pending permission/form request;
- `Background` to the native background operation;
- `Cancel` to exact turn/queued-input control where exposed;
- `Recover` to resume the known session after confirmed owner loss without replay;
- `Refresh`, `Result` and `Reconcile` to read-only native state/history.

Remove built-in host execution, `legacy_worker_demand`, `native.opencode.loop_step`
and public `native.mcp.*` phases. MCP plugin setup is adapter-internal; readiness is
reported asynchronously and never gates first dispatch.

Associate result output by explicit native input/parent ID where available. Otherwise
serialize one admitted input window per session, retain the turn baseline and settle
it at the matching terminal event. A stream gap, second independent input or changed
generation requires readback; never select the newest assistant message by guess.

#### Codex

Use the current Rust app-server adapter only. Map native Goal, live configure, reply,
turn interrupt, supported session recovery and account usage from the current
app-server protocol. Delete the retired Python executor/SDK and all Codex
artifact-version templates. The adapter's local rollout reader remains available
independently of the control socket.

#### Muse

Choose Node or Rust by complete behavior and total maintained Rust + JS + IPC code.
Retain one. Native Goal, turns, explicit recovery, usage and local session-log
observation stay in that adapter. Do not import the SDK's high-level recovery
coordinator beside Eliot's Operation readback. Delete the rejected executor and
duplicate normalizer.

#### Claude

Use one Rust adapter with a thin SDK driver. Map the published live controls that are
actually needed; native Goal remains unsupported on the reviewed interface, so Eliot
Goal messages remain available. Reuse official local session readers where they
reduce code, but do not create a transcript mirror merely for monitoring.

#### Command ACP and Antigravity

Map ACP prompt/reply/cancel/close to the common actions using the existing ACP crate.
Batch remains a limited profile without fake live controls. Antigravity keeps one
warm-stream implementation and ordinary result-window association; vendor session
proof structures do not enter the shared contract.

### P6 — Preserve the two Goal mechanisms

Harness Goal is `Goal` in the typed adapter port and is used by default where the
adapter declares it. The adapter maps set/edit/pause/resume/clear and records native
state/turns against the initiating Operation. Native set/resume may start work; avoid
sending the initial assignment a second time.

Eliot Goal stays in Store [goals](../../crates/swarm-kernel-host/src/store/goals.rs)
and becomes a small automation action:

```text
selected event or due occurrence
    -> current Goal and assignment
    -> ordinary Send message
    -> one admitted Operation
```

Reuse the existing scheduler and command bus. Add only message text/template,
trigger/cadence and delivery mode to the current plugin configuration. Remove
provider branches, OpenCode pseudo-Goal `continue`, objective approval hashes and
Goal-specific continuation receipts from
[automation_goal_progression](../../crates/swarm-kernel-host/src/store/automation_goal_progression.rs).

Replaying one occurrence returns the same Operation; a later occurrence may
intentionally repeat the text. Downtime coalesces missed ticks. Disabling the plugin,
pausing native Goal, cancelling a turn and clearing queued input remain independent.

### P7 — Passive observation and quotas

Implement the already defined [local view](observability.md#5-live-monitoring-and-future-charts)
through one bounded host collection lifecycle, independent of automation and optional
diagnostics.

Adapters fold native events and read provider-local JSON/JSONL/SQLite using the
selected runtime's known data root. Host adds OS process and Git facts. Store joins:

- Task/Attempt/Operation and binding when known;
- native session/turn/child identity;
- adapter and external service process identity;
- worktree, actual branch/HEAD, upstream and push target;
- last independently observed GitHub repository/ref/head;
- local provider source/cursor and source gaps;
- account usage/limit snapshots.

Configured sources start readers without a Task or binding. Unmatched sessions and
children are inventory, not assigned work. Collection never grants control rights.
`agent.list`, `agent.state`, `agent.family` and `agent.usage` read the Store projection
without provider calls.

Use selective `sysinfo` refresh in the existing process owner and preserve exact OS
birth identity. Remove per-sample executable hashing and the eight-process diagnostic
limit from fleet observation. Share one service's metrics and one account quota
snapshot instead of multiplying them per session.

Provider-local readers are bounded by bytes, records and elapsed work. Preserve
complete-record cursors and a bounded unfinished suffix. A failed/filtered scan is a
coverage gap, not an empty fleet. SQLite readers use one short read-only WAL-visible
connection per store; no `immutable=1`, repair or private writes.

Fix Store startup so every unreleased binding loses stale connection health, not
only `ready`. Preserve source identity in timeline/follow events. A late native
sequence may be stored for history without replacing newer current state.

Fix `NativeUsageSnapshot` admission to validate the selected adapter/source contract,
not an allowlist of bridge artifact versions. Malformed optional usage cannot abort
valid activity/result observations. Preserve native units and freshness; absent quota
is unavailable, not zero.

### P8 — Launch, result, review and acceptance

Make launch preview optional. Store admits explicit settings and expected revisions,
resolves one launch plan under the Operation and references that plan from workspace
and dispatch. Remove caller-supplied plan hashes and repeated comparisons. Keep
current authority/revision/workspace checks.

Remove the OpenCode/Muse MCP readiness prerequisite from first dispatch. A prepared
workspace, current assignment/binding and usable native session are sufficient.
MCP discovery is asynchronous observation.

Keep one result path:

```text
Result command -> adapter page -> module.result -> immutable artifact
               -> artifact.read / artifact.assemble -> submission
```

The adapter retains an unacknowledged page until Store acknowledges it. File
publication precedes SQLite receipt; an orphan file after crash is not a committed
result. Do not add another result reader or assembler.

Move required independent-review evidence into the common acceptance gate. Resolve
the exact candidate's assigned review/check results, preserve the manager decision,
and remove caller-authored duplicate review-proof arrays. Corrected candidate B
requires B's own review. Checks-only policies remain possible.

### P9 — Store, automation and product deletion

Keep one Store writer. Move pure bounded query families to the existing WAL reader
pool; authorization, page data and journal cut use one short snapshot. Do not route
by method-name suffix, and do not hold a transaction across native/file/Git/network
I/O or client pagination.

Replace Store's base-SQL SHA equality with application identity, current schema
version and explicit bounded migrations. Convert retained indexed identity/state/due
fields from `meta`; keep bounded payload JSON in its owning row where useful. Remove
old key codecs and readers after consumers switch. Do not add an ORM, EAV store or
second migration runner.

Give retained automation an immutable `entry_id` independent of owner/name. Update
owner/revision in one transaction; journal/occurrence rows keep the same entry ID.
Delete successor copies, six relocation paths, transfer chains/cycle logic and
parallel schedulers. One `swarm-automation` service evaluates due work; Store commits
due identity and action disposition.

Remove from compact-core completion:

- Concilium/council state machine;
- generic integration negotiation/ratification/ACK engine;
- automatic publication/Issue-label projection;
- document-edition rights and `LegacyV1` acceptance branches;
- retired installers, provenance/checkout/compiler/cache equality and duplicate
  package validators;
- host dependencies on worker implementation crates after needed contracts move.

Retain explicit accepted-candidate publication, ordinary messages, independent
review, participant grants and manual optional integrations.

## 3. Finite transition

Backward compatibility is not a product feature. Before deleting a current protocol
or executor, enumerate concrete unsettled Operations and required retained results.
For each, finish/read back with the current owner, export the result, or create a new
Task revision/Attempt. Then remove the old producer, reader, schema branch,
configuration, fixture and package together.

Do not write a generic converter for hypothetical old data. Historical completed
records may remain readable as opaque history when the current Store projection
already contains the needed facts; they do not keep an executable or runtime branch
alive.

## 4. Verification

Run scoped checks for the changed boundary. The minimum behavioral cases are:

| Boundary | Required proof |
| --- | --- |
| Protocol 2 and hello | Two different current adapters use the typed command and common shell. Store rights still reject unauthorized calls. Adding an optional action changes one adapter capability, not Core artifact/version code. |
| Command cleanup | OpenCode steering uses `Send`; ACP cancellation uses `Cancel`; session close uses `Shutdown`; removed `native.*` public methods have no callers, schemas, capacity entries or docs. |
| Receipts | Authenticated stale/wrong binding/boot reports fail; same Operation re-delivery is idempotent; conflicting adapter-local replay fails. No package/version/request hash is needed to accept a valid outcome. Result bytes still verify and assemble. |
| Goals | Native Goal works alone by default; Eliot Goal works alone or alongside it. Plugin disable and native pause affect only their mechanism. One occurrence replays once; later occurrences may repeat text. |
| Observation | An unbound session, shared native server, surviving process after adapter loss, changed worktree and stale GitHub ref remain distinguishable without provider calls. PID reuse and source gaps do not produce false liveness/completion. |
| Results | Explicit-parent and serialized-turn-window outputs bind to the correct Operation. A native stream gap stays unresolved without resend. |
| Review | Manual and automated acceptance use the same retained candidate-specific review/check evidence; B cannot reuse A's review. |
| Store/automation | Restart/lost wakeup retains admitted work; WAL reads are authorized and bounded; A→B→A automation transfer preserves one entry; old scheduler/keys are gone. |
| Deletion | Removed paths have no config, package, CI, docs or fallback wrapper. Report net owned-code change across Rust, JS and glue, separating tests. |

Documentation changes require content/link checks. Runtime changes require the
minimal affected Clippy/build checks first; broader product tests follow when the
connected product path is complete.
