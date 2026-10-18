# Eliot Swarm Controller — Implementation

This is the single implementation program for the
[architecture](architecture.md). Source was rechecked at
`05b6d05828e4f3a9106c7c726034de714db845e8` on 2026-10-18. Source descriptions
name existing code; instructions describe the target unless marked completed.
[Findings](refactoring.md) preserve evidence and measured surfaces;
[status](../implementation-status.md) records completed changes and runtime receipts.

Do not add another roadmap, protocol family or compatibility registry. Complete one
connected slice from Store admission through the selected adapter and result/readback,
then delete the replaced path.

## 1. Ownership

| Responsibility | Owner |
| --- | --- |
| Tasks, Attempts, Operations, rights, reservations, acceptance and durable observations | [Host Store](../../crates/swarm-kernel-host/src/store/mod.rs). `swarm-store` owns SQLite mechanics, not domain policy. |
| Shared semantic adapter protocol | [swarm-contracts](../../crates/swarm-contracts/src/lib.rs). Current typed commands/events and bounds only; no vendor structs. |
| Authenticated Eliot IPC and exact Store-delivery outbox | [swarm-client](../../crates/swarm-client/src/lib.rs) and `ModuleLink`. |
| Private durable file primitives and adapter bootstrap identity | [swarm-process](../../crates/swarm-process/src/lib.rs) and [module_owner](../../crates/swarm-process/src/module_owner.rs). |
| Vendor transport, native IDs, session state, effect readback and provider-local readers | The selected `swarm-adapter-*` implementation. |
| Process/worktree/Git collection and joined public projection | Existing host lifecycle and Store read owners; optional diagnostics remain in `swarm-telemetry`/`swarm-observer`. |
| Scheduling enabled helpers | One `swarm-automation` service with due state committed by Store. |
| CLI and MCP | `swarm-cli` and `swarm-mcp`, both calling the same Store methods. |

No adapter stores Tasks or accepts candidates. No frontend interprets vendor events.
No common helper owns vendor retry, reconnect or session recovery policy.

## 2. How agents implement this program

### One compact slice packet

The lead manager reads historical specifications, audits, Claude scripts and donor
documentation. A coding worker receives only the current architecture plus one slice
packet:

```text
Objective
Observable behavior
Current source owners and exact symbols
Unsupported/failure cases
Old paths/configuration/fixtures to delete
Native API or donor mechanism used
Minimal format/Clippy/build command
Completion report: behavior, deletions, net owned LOC, unresolved facts
```

Use current symbol names, not line numbers. Do not send the entire Issue thread,
several audits or all donor documents into every worker's context. A policy change
creates a new Task/Attempt revision or replacement slice packet; it does not append
another prose addendum.

### Manager and worker split

One manager owns one worktree and the integrated candidate. The manager edits shared
contracts, Store state transitions and files touched by several slices. Workers get
narrow, non-overlapping leaf responsibilities. The historical microtask result — one
local responsibility, exact symbols and exact completion condition — is useful for
weak models, but never split one atomic state transition or common contract across
workers.

The manager integrates once and one independent verifier checks the changed boundary.
Do not run a full audit before delivery and another equivalent audit after delivery.
Do not create a branch, PR or worktree for every native subagent.

### Code-first loop

For an implementation slice:

1. Read the exact current source owner and the named native/donor source.
2. Edit the production path immediately; do not write a preparation report.
3. Delete the replaced caller/configuration/fixture in the same slice.
4. Run formatting and the minimal affected warnings-denied Clippy or syntax check.
5. Report behavior, removed paths, net owned Rust + JS + glue, and unresolved facts.
6. Add/run focused connected tests when the product path is complete; run broad
   qualification only when the product is ready.

A blocker report names the exact missing API, object identity or source evidence. It
does not propose a new framework as a substitute for finishing the slice.

### Anti-bloat gates

- A semantic product action may have one real consumer. A shared implementation
  abstraction requires at least two current consumers and deletes their duplicated
  code in the same change.
- New code is net-negative by default. Positive net code must implement a named
  missing behavior and remove the superseded mechanism within the connected slice.
- No new crate, service, broker, ORM, workflow language, plugin registry, generic
  repository, protocol generator or compatibility facade without a concrete current
  owner that it replaces.
- Rust types are the current internal contract. Do not maintain hand-written JSON
  schemas, duplicated DTO families or version ladders unless an external protocol
  actually requires them.
- Build concurrency follows host/Cargo capacity, not agent count. One manager/worktree
  uses one external target/cache policy; workers do not create independent build
  farms.
- `done` means caller-to-result behavior is connected and the old path is absent,
  not that interfaces, fixtures or future TODOs exist.

## 3. Implementation order

### P1 — Replace the module compatibility matrix with protocol 2

Edit:

- [module_command.rs](../../crates/swarm-contracts/src/module_command.rs)
- [runtime.rs](../../crates/swarm-contracts/src/runtime.rs)
- [module_contract.rs](../../crates/swarm-contracts/src/module_contract.rs)
- [module_catalog.rs](../../crates/swarm-contracts/src/module_catalog.rs)
- [module_handshake.rs](../../crates/swarm-kernel-host/src/store/module_handshake.rs)
- Store command admission and the public method catalog.

Introduce one typed internal `AdapterCommand` protocol. Public Store methods can keep
stable names, but adapters do not receive arbitrary method strings or vendor RPC
names.

```text
Open          open/resume/attach a selected native session
Dispatch      initial TaskPrompt for one Task/Attempt/Operation
Send          next-turn or exact active-turn steering input
Reply         answer one observed pending request
Configure     apply supported model/mode/options
Goal          set/edit/pause/resume/clear native Harness Goal
Background    move an eligible native foreground tool/run to background
Cancel        cancel one exact turn or queued input
Recover       re-establish one known native session after confirmed owner/connection loss
Refresh       explicit safe native refresh/readback
Result        retrieve output attributable to one admitted input
Reconcile     read back one uncertain admitted action
Shutdown      release or stop an explicitly owned native resource
```

Use enums/closed payloads for common fields. A vendor-only action remains private to
its adapter until it has a real controller consumer. Do not expose a generic
`native.*` execute method.

Remove from the common command set and Store public routing:

- `native.opencode.loop_step` → `Send { delivery: steer }` where supported;
- `native.command.cancel_turn` → `Cancel { target: turn }`;
- `native.command.close_session` → `Shutdown`;
- `native.command.release_evidence` → adapter-internal cleanup;
- `native.mcp.install|observe|arm|read` → OpenCode adapter-internal setup/observation.

Keep `Recover`: current Muse recovery restores a known session after confirmed owner
loss without replaying Task input. It is distinct from `Open` and from `Reconcile`.
Unsupported adapters reject it before native I/O.

Define one compact `AdapterCapabilities` value:

- supported actions;
- `next_turn` and/or exact `steer` delivery;
- cancel scopes (`turn`, `queued_input`);
- supported native Goal actions;
- optional background control;
- local activity, result and account-usage source availability.

Capabilities route commands and report unsupported behavior. Store rights are checked
separately. Do not encode package versions, schema permutations or package history.

### P2 — Simplify descriptor and hello

The installed descriptor remains package metadata: module ID, executable, launch
configuration, lifecycle, current protocol major and required semantic actions.
Installation may verify selected executable/package bytes once.

At process start:

1. `swarm-process::module_owner` verifies the process belongs to the exact
   supervisor-owned scope and loads binding, generation, client and credential.
2. The adapter reports protocol major and compiled `AdapterCapabilities`.
3. Store verifies that descriptor-required actions are a subset of compiled handlers
   and returns the selected subset.
4. `ModuleLink` marks the authenticated physical link ready.

Delete the full echo/compare of module ID, artifact version/build, config schema,
pre-input contract and command/event schema arrays on both sides. The supervisor
already selected the package. Keep package identity in diagnostics/inventory, not in
Store behavior branches.

Delete:

- `ModuleContractTemplate::codex_rust_controller_v1..vN` ladders;
- `CODEX_RUST_CONTROLLER_CURRENT_DESCRIPTOR_VERSION` as a behavior selector;
- hardcoded OpenCode/Command artifact-version checks in
  `selected_native_command_supported`;
- legacy selectorless module behavior after transition;
- exact capability/schema-set matchers copied into every Rust/JS adapter;
- internal command/event schema inventories replaced by protocol-2 Rust types.

Configuration is validated once by the current typed adapter config at install/start.
Do not negotiate its schema on every hello. A package version remains visible for
inventory and rollback; it cannot decide whether a semantic command is valid.

### P3 — Share bootstrap and Store delivery, not vendor state

Do not create an adapter-framework crate. Extend existing owners only after two
current adapters consume the same implementation.

`swarm-process::module_owner` gains one common bootstrap reader returning:

- verified owner/worker membership;
- binding ID and generation;
- module client ID and credential path;
- private state directory;
- selected protocol and required capabilities;
- typed host connection config path.

It does not parse vendor configuration, launch native processes or own session state.

`swarm-client` gains a small adapter loop around `ModuleLink`:

- connect and one hello per physical link;
- reconnect Eliot IPC;
- pull `module.next`;
- decode the closed `AdapterCommand` envelope;
- drain one exact `DeliveryOutbox`;
- bounded adapter-loop shutdown.

`DeliveryOutbox` contains only already-produced Eliot deliveries:

```text
operation_id
kind = outcome | observation | result
adapter boot/generation
exact bounded encoded envelope
optional content digest for conflict detection
```

It resends the exact envelope until Store acknowledges it, then removes it using the
existing private durable file primitives in `swarm-process`. It does not record native
intent, choose retries, reconstruct sessions, interpret completion or schedule work.

First prove bootstrap/outbox extraction in two lower-complexity current Rust adapters,
preferably Command ACP and Antigravity Rust; Claude is the alternate if one of those
cannot exercise the required link path. Delete their old outer connect/pull/delivery
loops immediately. Do not start the shared abstraction in Codex or Muse, whose large
vendor state machines would force provider details into the common shell.

After that, migrate Claude, standalone OpenCode, Codex and the selected Muse
implementation one at a time. A migration is complete only when its duplicate env
parsing, hello equality and Eliot-delivery loop are gone.

### P4 — Native idempotency first; local effect state only for gaps

Native effect recovery is adapter-specific. Before writing any local native-effect
journal, classify the selected harness:

| Harness | Preferred identity/readback | Consequence |
| --- | --- | --- |
| Muse | Same durable UUIDv7 `commandId`, identical params and cursor-bearing session log | Retry only the same known command under Muse's documented replay rules; otherwise read back. |
| OpenCode | Store-chosen inbox/message ID plus durable session inbox/log | Reusing the same matching ID converges on first admission; read projected inbox/message before any retry. |
| Codex | `clientUserMessageId`, exact `threadId`/`turnId`, `expectedTurnId` steer guard and thread/turn history | Use these native identities where documented. JSON-RPC request ID is connection-scoped and is not durable idempotency. |
| ACP/ACPX | Request/queue identity, prompt-start admission and retained watch/result | Request ID is not a replay API. Lost submission stays uncertain; inspect the session. |
| Claude | Session/resume identity and retained SDK/local history; no reviewed universal durable mutation key | Serialize effects per session and keep one small prepared record only where readback cannot classify the outcome. |
| Command batch / Antigravity | Process/run identity and captured output; no general native dedupe | One admitted effect at a time, exact local prepared record, no blind process restart/replay. |

The algorithm is:

1. Store admits one Operation and supplies binding/generation.
2. Adapter chooses and retains the native identity before I/O when the harness does
   not already derive it durably from the Operation.
3. Adapter performs one native call.
4. Lost response uses native readback first. Unknown remains unknown.
5. Once a bounded Eliot outcome/observation/result exists, place that envelope in
   `DeliveryOutbox`.
6. Store acknowledgment removes only the outbox delivery; it does not rewrite native
   history.

Do not create a universal native effect journal. Each adapter may retain one small
closed record containing only what its native API needs: Operation ID, native ID,
exact method/params or their bounded bytes, phase (`prepared`, `uncertain`, `settled`)
and readback identity. Use existing durable file operations; do not add another DB.

Simplify shared receipts. The authenticated link, binding/generation and retained
Operation identify who may report an outcome. Protocol 2 needs only:

- Operation ID;
- adapter boot/generation needed to reject stale reports;
- action disposition and optional native input/turn/request IDs;
- bounded action-specific details.

Delete or collapse:

- `ModuleReceiptIdentity` package/protocol/request repetition;
- duplicated Task/Attempt/revision and source-text hashes across dispatch and result
  structures;
- Goal continuation admission/receipt/evidence once Eliot Goal is ordinary `Send`;
- `AntigravityDispatchSessionProof` and other vendor readiness proofs in
  `swarm-contracts`;
- native MCP protected command envelopes from the public runtime protocol.

Keep hashes for actual immutable bytes: credentials/verifiers, selected package bytes
at install/launch and retained result/artifact content. Native model/MCP/readiness is
an observation, not a cryptographic admission token.

### P5 — Convert native execution and remove duplicates

#### OpenCode

Use the standalone Rust adapter as the only new-work executor. Map:

- `Open` to selected session create/resume;
- `Dispatch`/`Send` to durable inbox input using a Store-derived stable item ID;
- `Reply` to the exact pending permission/form request;
- `Background` to the native background operation;
- `Cancel` to exact turn/queued-input control;
- `Recover` to the known session after confirmed owner loss without replay;
- `Refresh`, `Result` and `Reconcile` to native session/log readback.

Remove built-in host execution, `legacy_worker_demand`, public loop-step and public
native MCP phases. MCP plugin setup is adapter-internal and cannot gate first input.

Associate output by explicit native parent/input ID where available. Otherwise
serialize one admitted input window per session, retain its baseline and settle at
the matching terminal event. A stream gap, second independent input or changed
generation requires readback; never choose the newest assistant message by guess.

#### Codex

Use the current Rust app-server adapter only. Map native Goal, live configure, reply,
exact steer/interrupt, supported recovery and account usage from app-server. Use
`clientUserMessageId` and thread/turn readback where the native contract supports
it; do not treat connection request IDs as durable replay keys.

Delete the retired Python executor/SDK and all Codex artifact-version templates. The
local rollout reader remains independently available when the control socket is lost.

#### Muse

Choose Node or Rust by complete behavior and total maintained Rust + JS + IPC code.
Retain one. Native Goal, turns, durable command IDs, explicit recovery, usage and
local session log stay in that adapter. Use the SDK's documented command/log
semantics, not its high-level product coordinator beside Eliot Store. Delete the
rejected executor and duplicate normalizer.

#### Claude

Use one Rust adapter with a thin current SDK driver. Map only published controls
needed by Eliot. Native Goal remains unsupported on the reviewed interface, so Eliot
Goal messages remain available. Reuse official local session readers where they
reduce code; do not build a transcript mirror or duplicate permission engine.

#### Command ACP and Antigravity

Map ACP prompt/reply/cancel/close to common actions through the existing ACP crate.
Batch remains a limited profile without fake live controls. Antigravity retains one
warm-stream implementation and one sequential input/result window. Vendor readiness
proofs do not enter shared contracts.

<a id="harness-goals-and-the-eliot-goal-plugin"></a>

### P6 — Preserve the two Goal mechanisms

Harness Goal is `Goal` in the typed adapter port and is used by default where the
adapter declares it. Map set/edit/pause/resume/clear and record native state/turns
against the initiating Operation. Native set/resume may start work; do not send the
initial assignment twice.

Eliot Goal stays in Store [goals](../../crates/swarm-kernel-host/src/store/goals.rs)
and becomes one small automation action:

```text
selected event or due occurrence
    -> current Goal and assignment
    -> ordinary Send message
    -> one admitted Operation
```

Reuse the existing scheduler and command bus. Add only message text/template,
trigger/cadence and delivery mode. Remove provider branches, OpenCode pseudo-Goal
`continue`, objective approval hashes and Goal-specific continuation receipts from
[automation_goal_progression](../../crates/swarm-kernel-host/src/store/automation_goal_progression.rs).

Replaying one occurrence returns the same Operation; a later occurrence may repeat
the text intentionally. Downtime coalesces missed ticks. Plugin disable, native Goal
pause, turn cancellation and queued-input removal remain independent.

<a id="passive-agent-observation"></a>

### P7 — Passive observation and quotas

Implement the [local view](observability.md#5-live-monitoring-and-future-charts)
through one bounded host collection lifecycle, independent of automation and optional
diagnostics.

Adapters fold native events and read provider-local JSON/JSONL/SQLite using the
runtime's known data root. Host adds OS process and Git facts. Store joins:

- Task/Attempt/Operation and binding when known;
- native session/turn/child identity;
- adapter and shared-service process identity;
- worktree, actual branch/HEAD, upstream and push target;
- last independent GitHub repository/ref/head;
- provider source/cursor and explicit coverage gaps;
- account usage/limit snapshots.

Configured sources start readers without a Task or binding. Unmatched sessions and
children remain inventory, not assigned work. `agent.list`, `agent.state`,
`agent.family` and `agent.usage` read Store without provider calls.

Preserve exact OS birth identity. Remove per-sample executable hashing and the
eight-process diagnostic limit from fleet observation. Share one service's metrics
and one account quota snapshot instead of multiplying them per session.

Bound bytes, records and elapsed work. Preserve complete-record cursors and one
bounded unfinished suffix. Failed discovery is a coverage gap, not an empty fleet.
SQLite readers use one short read-only WAL-visible connection; no `immutable=1`,
repair or private writes.

Fix startup so every unreleased binding loses stale connection health, not only
`ready`. Preserve source identity in follow/timeline events. Late native sequence may
remain history without replacing newer current state.

Validate `NativeUsageSnapshot` against the selected adapter/source contract, not
artifact-version allowlists. Malformed optional usage cannot discard valid activity
or result observations. Missing quota is unavailable, not zero.

### P8 — Launch, result, review and acceptance

Make launch preview optional. Store admits explicit settings and expected revisions,
resolves one launch plan under the Operation and references it from workspace and
dispatch. Remove caller-supplied plan hashes and repeated comparisons. Keep current
authority/revision/workspace checks.

Remove OpenCode/Muse MCP readiness from first dispatch. A prepared workspace, current
assignment/binding and usable native session are sufficient. MCP discovery is
asynchronous observation.

Keep one result path:

```text
Result command -> adapter page -> module.result -> immutable artifact
               -> artifact.read / artifact.assemble -> submission
```

The exact result envelope remains in `DeliveryOutbox` until Store acknowledgment.
File publication precedes SQLite receipt; an orphan file after crash is not a
committed result. Do not add another result reader or assembler.

Move required independent-review evidence into the common acceptance gate. Resolve
the exact candidate's assigned review/check results and preserve the manager decision.
Remove caller-authored duplicate review-proof arrays. Candidate B requires B's own
review. Checks-only policies remain possible.

<a id="store-and-current-data"></a>

### P9 — Store, automation and product deletion

Keep one Store writer. Move pure bounded query families to the existing WAL reader
pool; authorization, page data and journal cut use one short snapshot. Do not route
by method-name suffix or hold a transaction across native/file/Git/network I/O or
client pagination.

Replace base-SQL text SHA equality with application identity, current schema version
and explicit bounded migrations. Convert retained indexed identity/state/due fields
from `meta`; keep bounded payload JSON in its owning row where useful. Remove old key
codecs/readers after consumers switch. Do not add an ORM, EAV store or second
migration runner.

Give retained automation immutable `entry_id` independent of owner/name. Update
owner/revision in one transaction; journal/occurrence rows keep the same ID. Delete
successor copies, six relocation paths, transfer chains/cycle logic and parallel
schedulers. One `swarm-automation` service evaluates due work; Store commits due
identity and action disposition.

Remove from compact-core completion:

- Concilium/council state machine;
- generic integration negotiation/ratification/ACK engine;
- automatic publication/Issue-label projection;
- document-edition rights and `LegacyV1` acceptance branches;
- retired installers, provenance/checkout/compiler/cache equality and duplicate
  package validators;
- host dependencies on worker implementation crates after required contracts move.

Retain explicit accepted-candidate publication, ordinary messages, independent
review, participant grants and manual optional integrations.

<a id="current-format-and-existing-data"></a>

## 4. Finite transition

Backward compatibility is not a product feature. Protocol transition has one branch:

- all newly created bindings and work use protocol 2;
- protocol 1 exists only for an enumerated set of current unsettled Operations;
- protocol 1 receives no new capabilities, adapters or format variants;
- each retained Operation is finished/read back, exported or replaced by a new Task
  revision/Attempt;
- then protocol-1 producers, readers, descriptors, fixtures and config are deleted
  together.

Do not branch on artifact version inside a protocol major and do not write a generic
converter for hypothetical old data. Historical completed records may remain opaque
history when current Store projection already contains needed facts; they do not keep
an executor or compatibility shim alive.

## 5. Verification

During implementation run formatting and the minimal warnings-denied Clippy/build or
syntax check for affected packages. Do not create a broad test matrix around an
incomplete path. When the connected path is complete, verify these boundaries; run
full native/release qualification when the product is ready.

| Boundary | Required proof |
| --- | --- |
| Agent slice | Worker changed only assigned owners, manager integrated once, old path was deleted, report includes net owned code. |
| Protocol 2 and hello | Two current adapters use typed commands. Store rights reject unauthorized calls. Optional action changes capability, not artifact/version code. |
| Store delivery outbox | Lost Store ACK resends identical envelope; acknowledgment removes it; outbox never replays native input. |
| Native effect recovery | Muse/OpenCode converge through native IDs; Codex uses exact thread/turn readback; ACP unknown is not retried; adapters without native dedupe serialize one prepared effect. |
| Command cleanup | OpenCode steer uses `Send`; ACP cancel uses `Cancel`; close uses `Shutdown`; removed `native.*` methods have no callers, schemas, capacity rows or docs. |
| Receipts | Stale/wrong binding/boot report fails; same Store delivery is idempotent; no package/version/request hash is required; result bytes still verify. |
| Goals | Native Goal and Eliot Goal work separately or together. Plugin disable and native pause affect only their mechanism. |
| Observation | Unbound session, shared server, surviving process, changed worktree and stale remote ref remain distinguishable without provider calls. |
| Results | Explicit-parent and serialized-turn-window output binds to the correct Operation; stream gap remains unresolved without resend. |
| Review | Manual and automated acceptance use the same candidate-specific evidence; B cannot reuse A's review. |
| Store/automation | Restart/lost wakeup retains admitted work; WAL reads are authorized/bounded; automation transfer preserves one entry; old schedulers/keys are gone. |
| Deletion | Removed paths have no config, package, CI, docs or fallback wrapper. |

Documentation changes require content/link checks. Every completed source slice updates
[status](../implementation-status.md) with behavior, evidence and net owned-code
change.
