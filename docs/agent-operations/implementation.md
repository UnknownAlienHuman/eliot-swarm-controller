# Eliot Swarm Controller — Implementation

This is the single implementation program for the
[architecture](architecture.md). Runtime source was rechecked at
`05b6d05828e4f3a9106c7c726034de714db845e8` on 2026-10-18; later commits in this
series change documentation only. [Findings](refactoring.md) preserve evidence;
[status](../implementation-status.md) records implemented source and runtime receipts.

Do not add another roadmap, protocol family or compatibility registry. Work in
vertical caller-to-result slices and delete the replaced path in the same slice.

## 1. How agents execute this program

### Slice packet

The lead manager reads historical specifications, audits, Claude scripts and donor
documentation. A coding worker receives the current architecture plus one compact
packet:

```text
Objective and observable behavior
Current source owners and exact symbols
Unsupported and failure cases
Old callers/configuration/fixtures to delete
Named native API or donor mechanism
Minimal format/Clippy/build command
Report: behavior, deletions, net owned LOC, unresolved facts
```

Use symbols and current paths, not stale line numbers. Do not send the full Issue,
several audits or the whole donor catalogue to every worker. Changed policy creates a
new Task/Attempt revision or replacement packet, not another addendum or daemon.

### Ownership and integration

One manager owns one worktree and the integrated candidate. Shared contracts, Store
state transitions and files touched by several slices stay with that manager. Workers
receive narrow, non-overlapping leaf work. A weak-model microtask may cover one local
responsibility, exact symbols and one observable completion condition; never split one
atomic state transition between workers.

The manager integrates once. One independent verifier checks the changed boundary.
Do not run equivalent full audits before and after delivery, and do not create a PR,
branch or worktree for every native subagent.

### Code-first and anti-bloat gates

1. Read the exact current source owner and named native/donor source.
2. Edit the production path immediately; do not write a preparation report.
3. Delete the replaced caller, configuration, fixture and compatibility branch.
4. Run formatting and the minimal affected warnings-denied Clippy or syntax check.
5. Report behavior, deletion, net owned Rust + JS + glue, and unresolved facts.
6. Add/run focused connected tests when the path is complete; run broad native and
   release qualification when the product is ready.

A semantic product action may have one real consumer. A shared implementation
abstraction requires two current consumers and must delete both duplicated mechanisms
in the same migration. No new crate, service, broker, ORM, workflow language, plugin
registry, generic repository, protocol generator or compatibility facade without a
concrete current owner that it replaces.

Rust types are the current internal contract. Do not maintain parallel hand-written
JSON schemas, DTO families or version ladders unless an external protocol requires
them. Build concurrency follows host/Cargo capacity, not agent count. `done` means the
caller-to-result behavior is connected and the old path is absent, not merely that
interfaces, fixtures or TODOs exist.

## 2. Owners

| Responsibility | Owner |
| --- | --- |
| Tasks, Attempts, Operations, rights, reservations, acceptance and durable observations | [Host Store](../../crates/swarm-kernel-host/src/store/mod.rs). `swarm-store` owns SQLite mechanics, not domain policy. |
| Shared semantic adapter protocol | [swarm-contracts](../../crates/swarm-contracts/src/lib.rs). Current typed actions/events and bounds only; no vendor structs. |
| Authenticated Eliot IPC and exact Store-delivery outbox | [swarm-client](../../crates/swarm-client/src/lib.rs) and `ModuleLink`. |
| Private durable files and adapter process identity | [swarm-process](../../crates/swarm-process/src/lib.rs) and [module_owner](../../crates/swarm-process/src/module_owner.rs). |
| Vendor transport, native identities, session state, effect readback and provider-local readers | The selected `swarm-adapter-*` implementation. |
| Process/worktree/Git collection and joined public projection | Existing host lifecycle and Store read owners; optional diagnostics remain in `swarm-telemetry`/`swarm-observer`. |
| Enabled helper scheduling | One `swarm-automation` service with due state committed by Store. |
| CLI and MCP | `swarm-cli` and `swarm-mcp`, both calling the same Store methods. |

No adapter stores Tasks or accepts candidates. No frontend interprets vendor events.
No common helper owns vendor retry, native reconnect or session recovery policy.

## 3. Route-by-route implementation order

P1–P4 are one vertical migration sequence, not four repository-wide preparation
programs. The first slice selects one current adapter route and wires only the actions
needed for assignment through result/readback. The second route proves any shared
bootstrap/outbox code. Other routes remain on their current protocol temporarily and
receive no new protocol-1 features or formats. Do not rewrite every descriptor,
receipt and adapter before one protocol-2 route works end to end.

### P1 — Typed semantic adapter port

Edit the current owners in:

- [module_command.rs](../../crates/swarm-contracts/src/module_command.rs)
- [runtime.rs](../../crates/swarm-contracts/src/runtime.rs)
- [module_contract.rs](../../crates/swarm-contracts/src/module_contract.rs)
- [module_catalog.rs](../../crates/swarm-contracts/src/module_catalog.rs)
- [module_handshake.rs](../../crates/swarm-kernel-host/src/store/module_handshake.rs)
- Store command admission and public method catalog.

The internal protocol is one closed `AdapterCommand` vocabulary:

```text
Open          open/resume/attach a selected native session
Dispatch      initial TaskPrompt for one Task/Attempt/Operation
Send          next-turn or exact active-turn steering input
Reply         answer one observed pending request
Configure     apply supported model/mode/options
Goal          set/edit/pause/resume/clear native Harness Goal
Background    move one eligible native foreground tool/run to background
Cancel        cancel one exact turn or queued input
Recover       restore one known session after confirmed owner/control loss
Refresh       explicit safe native readback
Result        retrieve output attributable to one admitted input
Reconcile     read back one uncertain admitted action
Shutdown      release or stop one explicitly owned native resource
```

The vocabulary may name the target actions, but a route migration defines payloads,
handlers and fixtures only for actions that route actually supports. Capability
absence represents unsupported behavior; do not generate placeholder handlers or
schemas for every variant.

Remove from common/public routing:

- `native.opencode.loop_step` → `Send { delivery: steer }`;
- `native.command.cancel_turn` → `Cancel { target: turn }`;
- `native.command.close_session` → `Shutdown`;
- `native.command.release_evidence` → adapter-internal cleanup;
- `native.mcp.install|observe|arm|read` → OpenCode adapter-internal setup/observation.

Keep `Recover`: Muse and other durable-session routes may restore a known session
without replaying Task input. It is distinct from `Open` and `Reconcile`.

`AdapterCapabilities` contains only supported actions, input delivery classes, cancel
scopes, native Goal verbs, optional background control and available local
activity/result/account-usage sources. It routes commands and reports unsupported
behavior; Store rights are checked separately. Do not encode package versions,
schema permutations or package history.

### P2 — Descriptor and hello

The installed descriptor contains package/launch metadata, lifecycle, current
protocol major and required semantic actions. Installation may verify selected
executable/package bytes once.

At process start:

1. `swarm-process::module_owner` verifies exact supervisor-owned process scope and
   loads binding, generation, module client and credential identity.
2. The adapter reports protocol major and compiled capabilities.
3. Store verifies that descriptor-required actions are a subset of compiled handlers
   and returns the selected subset.
4. `ModuleLink` marks the authenticated physical link ready.

Delete the full echo/compare of artifact version/build, config schema, pre-input
contract and command/event schema arrays. Package identity remains visible for
inventory and rollback, not Store behavior.

Delete Codex `ModuleContractTemplate::codex_rust_controller_v1..vN`, the current
version behavior selector, OpenCode/Command artifact-version checks, legacy
selectorless module behavior after transition, and copied capability/schema matchers
in every Rust/JS adapter. Typed adapter config is validated once at install/start; it
is not renegotiated on every hello.

### P3 — Common bootstrap and Store delivery only

Do not create an adapter-framework crate.

`swarm-process::module_owner` gains one common bootstrap reader returning verified
membership, binding/generation, module client/credential, private state directory,
selected protocol/requirements and typed host connection path. It does not parse
vendor config, launch native processes or own sessions.

`swarm-client` gains a small loop around `ModuleLink`: connect/hello, Eliot IPC
reconnect, `module.next`, closed command decoding, exact Store-delivery drain and
bounded shutdown.

`DeliveryOutbox` stores only already-produced Eliot envelopes:

```text
delivery_id and dependency lane
operation_id or observation source
kind = outcome | observation | result
binding/generation and adapter boot
exact bounded encoded envelope
optional local conflict/corruption digest
```

Outbox invariants:

- scope files by binding, generation and adapter boot;
- the same live boot may drain after Eliot IPC reconnect;
- a replacement boot never adopts or sends predecessor files automatically;
- native recovery/readback may produce a new current-boot envelope for the original
  Operation, but common outbox code cannot decide that;
- preserve order per Operation/result page and per ordered observation source; no
  global ordering between unrelated lanes;
- a permanent Store rejection stays visible and blocks only its dependency lane;
- bound records and bytes; stop pulling new commands and drain before capacity is
  exhausted, reserving room for the mandatory terminal record of admitted work;
- fold/coalesce high-volume native frames in the adapter before creating Store
  envelopes; the outbox is not a transcript or event mirror.

Use existing private durable file primitives in `swarm-process`; do not add an adapter
database. Select the first two consumers only after measuring duplicated
delivery-only code and confirming that their outer loops can be deleted without
moving vendor policy into the shell. Antigravity Rust and the current Command adapter
are initial candidates; choose the concrete Command profile after source tracing.
Claude is an alternate. Do not begin in Codex or Muse, whose large native state
machines would distort the common boundary.

### P4 — Native identity first; local effect state only for gaps

Before writing local native-effect state, classify the harness:

| Harness | Preferred identity/readback | Rule |
| --- | --- | --- |
| Muse | Durable UUIDv7 `commandId`, identical params, cursor-bearing session log | Retry only the same command under documented Muse replay rules; otherwise read back. |
| OpenCode | Store-chosen inbox/message ID and durable inbox/session log | Reusing the same matching ID converges on first admission; inspect inbox/message before retry. |
| Codex | `clientUserMessageId`, exact thread/turn IDs, `expectedTurnId` and thread/turn history | Use these identities where documented. JSON-RPC request ID is connection-scoped, not durable dedupe. |
| ACP/ACPX | Queue/request identity, prompt-start admission and watch/result | Request ID is not replay authority. Lost submission stays uncertain. |
| Claude | Session/resume identity and retained SDK/local history | Serialize per session and retain one small prepared record only where readback cannot classify the outcome. |
| Command batch / Antigravity | Process/run identity and captured output | One admitted effect window, exact prepared record, no blind process restart/replay. |

Algorithm:

1. Store admits one Operation and supplies binding/generation/action.
2. Adapter chooses and durably retains the native identity before I/O when needed.
3. Adapter performs one native call.
4. Lost response uses native readback first; unknown remains unknown.
5. Once a bounded Eliot outcome/observation/result exists, enqueue the exact envelope
   in `DeliveryOutbox`.
6. Store acknowledgment removes only that delivery; it does not rewrite native state.

Do not create a universal native effect journal. An adapter may retain one small
closed record with Operation ID, native ID, exact bounded method/params, phase and
readback identity. Use existing durable files, not another database.

Simplify receipts. Store validates the authenticated link, exact retained Operation,
expected semantic action, legal state transition and permitted generation/boot. The
adapter reports Operation ID, generation/boot, disposition, optional native IDs and
bounded action details. It does not repeat package/protocol/request identity.

Delete duplicated Task/Attempt/source hashes, Goal-continuation proof families,
`AntigravityDispatchSessionProof` and public native-MCP envelopes. Keep hashes for
credentials/verifiers, selected package bytes and retained result/artifact content.

### P5 — One executor per runtime

| Runtime | Current target and deletion |
| --- | --- |
| OpenCode | Keep standalone Rust adapter. Use durable inbox IDs, exact reply/cancel/background and native readback. Remove built-in host executor, `legacy_worker_demand`, public loop-step and public native-MCP phases. Serialize one admitted turn window when no explicit parent exists. |
| Codex | Keep current Rust app-server adapter. Map native Goal, configure, reply, exact steer/interrupt, recovery and usage. Use client message/thread/turn identity; delete Python executor/SDK and artifact-version templates. Local rollout reader remains available without the socket. |
| Muse | Select Node or Rust by complete behavior and total maintained Rust + JS + IPC. Keep native Goal, durable command IDs, explicit recovery, usage and session log in the winner; delete the other executor and duplicate normalizer. |
| Claude | Keep one Rust adapter with a thin current SDK driver. Map only required published controls; native Goal remains unsupported, so Eliot Goal remains available. Reuse official local readers without a transcript mirror or second permission engine. |
| Command ACP/batch | Use existing ACP crate for prompt/reply/cancel/close. Batch remains limited; remove historical profile ladders and fake live controls. |
| Antigravity | Keep one warm-stream implementation and one sequential input/result window. Readiness/model facts are observations, not shared proof packets. |

<a id="harness-goals-and-the-eliot-goal-plugin"></a>

### P6 — Native Goal and Eliot Goal

Harness Goal is `Goal` in the adapter port and is the default where supported. Map
set/edit/pause/resume/clear and associate native state/turns with the initiating
Operation. Native set/resume may start work; do not send the initial assignment twice.

Eliot Goal remains a small Store plugin:

```text
selected event or due occurrence
    -> current Goal and assignment
    -> ordinary Send message
    -> one admitted Operation
```

Reuse the existing scheduler/command bus. Configuration needs only message/template,
trigger/cadence and delivery mode. Remove provider branches, OpenCode pseudo-Goal,
objective approval hashes and Goal-specific continuation receipts. Replaying one
occurrence returns the same Operation; a later occurrence may intentionally repeat
the text. Plugin disable, native Goal pause, turn cancel and queued-input removal are
independent.

<a id="passive-agent-observation"></a>

### P7 — Passive observation and quotas

Use one bounded host collection lifecycle independent of automation and optional
diagnostics. Adapters fold native events and provider-local JSON/JSONL/SQLite; host
adds OS process and Git facts; Store joins assignment/binding, native session/turn,
process, worktree/branch/HEAD/upstream/push target, remote ref, source cursor/gaps and
account usage.

Configured sources start readers without a Task/binding. Unmatched sessions and
children remain inventory. `agent.list`, `agent.state`, `agent.family` and
`agent.usage` read Store without provider calls.

Keep assignment, activity, pending request/tool/child wait, connection, process
liveness, source freshness, Git state, Task completion and quota as separate facts.
Never kill/restart/reassign from age, low CPU, old provider status, unchanged branch,
PID alone, newest file or title/path regex.

Bound actual bytes, records and elapsed work. Preserve complete-record cursors and a
bounded unfinished suffix. Failed discovery is a visible coverage gap, not an empty
fleet. SQLite uses short read-only WAL-visible connections, never `immutable=1` for a
live database. Remove per-sample executable hashing and share one service's metrics
and one account quota snapshot.

Fix startup so every unreleased binding loses stale connection health, preserve
source identity in follow/timeline events, and validate usage by selected
adapter/source contract rather than artifact versions. Malformed optional usage
cannot discard valid activity/result observations. Missing quota is unavailable.

### P8 — Launch, results, review and acceptance

Launch preview is optional. Store admits explicit settings/expected revisions,
resolves one launch plan under the Operation and references it from workspace and
dispatch. Remove caller-supplied plan hashes and MCP-readiness gates from first input.

Keep one result path:

```text
Result -> adapter page -> module.result -> immutable artifact
       -> artifact.read / artifact.assemble -> submission
```

Associate output with explicit native parent/input identity or one serialized admitted
turn window. Changed generation, second independent input or unclassified stream gap
requires readback; never choose the newest assistant message by guess. The exact page
stays in `DeliveryOutbox` until Store acknowledgment. File publication precedes the
SQLite receipt; `artifact.assemble` is the only multi-page assembler.

Manual and automatic acceptance use the same candidate-specific retained review/check
gate. Remove caller-authored duplicate proof arrays. Candidate B cannot reuse A's
review; checks-only policies remain possible.

<a id="store-and-current-data"></a>

### P9 — Store, automation and product deletion

Keep one Store writer. Move pure bounded query families to the existing WAL reader
pool; authorization, page data and journal cut use one short snapshot. Never hold a
transaction across native/file/Git/network I/O or client pagination.

Replace base-SQL text SHA equality with application identity, current schema version
and explicit bounded migrations. Move indexed identity/state/due fields out of
`meta`; retain bounded payload JSON where useful. Remove old key codecs/readers after
consumers switch. No ORM, EAV store or second migration runner.

Give automation an immutable `entry_id` independent of owner/name. Update owner and
revision in one transaction; journals/occurrences retain the ID. Delete successor
copies, relocation paths, transfer chains/cycle logic and parallel schedulers. One
`swarm-automation` service evaluates due work; Store commits due identity and action
disposition.

Remove from compact-core completion:

- Concilium/council state machine;
- generic integration negotiation/ratification/ACK engine;
- automatic publication/Issue-label projection;
- document-edition rights and `LegacyV1` acceptance branches;
- retired installers, checkout/compiler/cache provenance equality and duplicate
  package validators;
- host dependencies on worker implementation crates after required contracts move.

Retain explicit accepted-candidate publication, ordinary messages, independent
review, participant grants and manual optional integrations.

<a id="current-format-and-existing-data"></a>

## 4. Finite protocol transition

Migration is route-by-route, not a repository-wide flag day:

- an unmigrated current route may continue protocol 1 for current/new work while it
  is the only available implementation, but receives no new protocol-1 features,
  schema variants or artifact profiles;
- once a route is migrated, all new bindings on that route use protocol 2 and the old
  producer is disabled for new work;
- concrete unsettled protocol-1 Operations for that route are finished/read back,
  exported or replaced by a new Task revision/Attempt;
- then that route's protocol-1 producer, reader, descriptor, fixture and config are
  deleted together;
- after the final route migrates, no new protocol-1 binding is admitted and the
  remaining enumerated old bindings are retired.

Do not branch on artifact version inside a protocol major and do not build generic
converters for hypothetical old data. Historical completed records may remain opaque
history when current Store projection contains the needed facts; they do not keep an
executor or compatibility shim alive.

## 5. Verification

During construction run formatting and minimal affected warnings-denied Clippy/build
or syntax checks. Do not create a broad matrix around an incomplete route. When the
connected path is complete, verify:

| Boundary | Required proof |
| --- | --- |
| Agent slice | Worker changed only assigned owners; manager integrated once; old path is gone; report contains net owned code. |
| Route migration | One protocol-2 route works assignment→result while unmigrated routes stay unchanged; second route proves shared code. |
| Typed port/hello | Store rights reject unauthorized calls; optional action changes capability, not artifact/version logic. |
| Store outbox | Same-boot reconnect resends identical envelope; ACK removes it; new boot cannot drain predecessor files; per-lane order and capacity backpressure hold; native input is never replayed. |
| Native effects | Muse/OpenCode converge through native IDs; Codex uses exact thread/turn readback; ACP unknown is not retried; no-dedupe adapter serializes one prepared effect. |
| Command cleanup | Steer/cancel/close use semantic actions; removed `native.*` methods have no callers, schemas, capacity rows or docs. |
| Receipts | Wrong action/state/binding/boot fails; package/version/request hashes are unnecessary; result bytes still verify. |
| Goals | Native and Eliot Goal work alone or together; disabling/pausing one affects only that mechanism. |
| Observation | Unbound session, shared server, surviving process, changed worktree and stale remote ref remain distinct without provider calls. |
| Results/review | Explicit-parent or turn-window result binds to the correct Operation; stream gap stays unresolved; B cannot reuse A's review. |
| Store/automation | Restart/lost wakeup retains admitted work; WAL reads are authorized/bounded; transfer preserves one entry; old schedulers/keys are gone. |
| Deletion | Removed path has no config, package, CI, docs, fixture or fallback wrapper. |

Documentation changes require content/link checks. Every completed source slice updates
[status](../implementation-status.md) with behavior, evidence and net owned-code
change.
