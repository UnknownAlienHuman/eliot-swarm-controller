# Refactoring — Source Findings

Runtime source was rechecked through
`8355bdf0fbe561b5163bd3ab1ba6db24c0f8ddbc` on 2026-10-19.
[Architecture](architecture.md) owns the product boundary;
[implementation](implementation.md) owns the only work order;
[status](../implementation-status.md) records completed source changes.

Source presence is not a requirement to preserve a subsystem.

## Size and concentration

| Owned Rust scope | Physical lines after CLI deletion | Explicit test-only estimate |
| --- | ---: | ---: |
| All owned Rust, excluding `vendor/` | 533,720 | 172,511 |
| `crates/swarm-kernel-host` | 365,726 | 128,996 |

Counts include blanks/comments. They describe concentration, not reachable production
SLOC. Commit `25a8e3b` removed 1,206 net Rust lines from duplicate host CLI. Later
changes through the source revision above are documentation only.

## Confirmed structural causes

### 1. Vendor/internal commands leaked into the common API

Common contracts, Store, policy, capacity and adapters expose OpenCode loop-step,
Command cancel/close/release-evidence and native MCP setup phases. These are semantic
Send/Cancel/Shutdown or adapter internals. Each leaked method creates schemas,
receipts, policy, capacity and fixtures.

### 2. Package metadata became behavior

Store and adapters exact-compare artifact version/build, protocol, config and command/
event schema inventories. Optional actions therefore create new package profiles and
compatibility branches. Package identity should remain installation/inventory data;
Store object rights remain authority.

### 3. Bootstrap and Eliot delivery are reimplemented per adapter

Codex, Claude, OpenCode, Muse, Command and Antigravity repeat env/owner/credential
parsing, hello/claim equality, command pulling, reconnect and Store-delivery retry.
Existing `swarm-process::module_owner` and `swarm-client::ModuleLink` already own the
useful boundary. Extract only after two current routes delete their old loops.

### 4. Receipts repeat Store-owned identity

Package/protocol, binding/generation, Operation/request hashes, Task/Attempt/revision,
source/prompt hashes and nested receipts recur through dispatch, outcomes and results.
Store created the Operation and authenticated the link. Keep stale generation/boot,
observed native IDs/disposition and hashes on actual immutable bytes; delete echoes.

### 5. Vendor proof packets became a second admission protocol

Antigravity session proof, Goal continuation proofs and OpenCode MCP protected
envelopes are observations or adapter internals. They do not grant dispatch rights.

### 6. Antigravity can lose a native terminal outcome

Pending receipts are in memory and outcome depends on a Store observation ID. Native
completion can survive while Store remains `outcome_unknown` after worker exit. First
make outcome self-contained, then persist the exact Store envelope; never persist or
replay native input in that spool.

### 7. Parallel executors remain the largest adapter deletion

| Runtime | Current duplication | Target |
| --- | --- | --- |
| OpenCode | built-in host plus standalone Rust | standalone Rust |
| Muse | Node plus Rust | select one complete implementation |
| Codex | Rust plus retired Python SDK/fixtures | Rust; only required historical result readback |
| Claude | Rust/SDK plus retained Node generations | one Rust adapter |
| Antigravity | Node generations plus Rust | one warm-stream route |
| Command | Batch v3/v4/v5 and ACP v1/v2 | one current Batch and ACP generation |

Historical records do not justify an executable.

### 8. Observation is incomplete and partly expensive

Binding-only inventory misses unmatched sessions; process identity is dropped before
projection; metric samples rehash executables; source identity and restart connection
facts are incomplete; remote-ref reads depend on publication. Governor reads need one
joined local projection without provider calls.

### 9. WorkDispatch is another workflow engine

It copies blocked Tasks into cursors/pending lists, rechecks them, creates semantic
slots/links and quarantines ordinary blockers. A fixed 128 pending limit can hide
later eligible Tasks. Current Task state plus one `dispatch_once` admission is enough.

### 10. Advisory code scopes duplicate workspace authority

`code.scope.*` has proposals, acceptance, modes, symbols, digests, overrides, expiry,
indexes and conflict pages but does not lock files or start work. Workspace lease
already retains Task/Attempt/worktree/paths and rejects overlap before effects.

### 11. Process owner repeats the compatibility matrix

Owner/resolver records repeat package ID/version/build, protocol and full contract in
addition to binding and process identity. Keep exact process custody and private
credential reference; remove runtime package negotiation.

### 12. Capacity is a second Operation lifecycle database

Capacity materializes native Operations/producers into phases, event refs, unknown
timestamps and `meta` indexes and counts refresh/result/reconcile. Root capacity is
derivable transactionally from unresolved root opens and unreleased bindings.

### 13. Provider state is a vendor proof matrix

Shared DTOs know exact Codex/Muse methods, auth SHA, artifact allowlists and repeated
route/binding/model coordinates. Stale source bindings can create sticky holds and
Muse Rust is rejected. Use source-scoped availability/usage with honest freshness.

### 14. Three scheduler/control systems overlap

An in-process scheduler, standalone automation scheduler service and another
short-interval automation loop handle overlapping due/current work, while legacy
worker slots add lifecycle machinery. Store due state and one in-process wake/timer
loop are sufficient.

### 15. TaskPrompt sends controller internals to the model

Prompt text includes canonical identity JSON, Task brief and launch packet while
TaskPrompt envelope/context/results repeat Task/Attempt and source hashes. The model
needs current actionable work, not proof packets. Use one compact renderer and retain
identity in Store.

### 16. Review/correction is another workflow family

ReviewDisposition, RepairDispatch, semantic slots, operation links and separate
Acceptance repeat one candidate/review/manager relation. `review.submit`,
`task.request_changes` and the common acceptance gate already own the real state.

### 17. Public API is maintained in parallel registries

Method registry, MCP catalog/profiles/schemas, mutation field validation, Store role
router, apply/read dispatch, participant exceptions and visibility SQL all encode
parts of the same surface. No bypass was proven in this pass; the confirmed defect is
structural drift and removal cost. Split public/internal/adapter protocols and give
each retained public domain one typed authorizer/handler.

### 18. Store migration verifies SQL source text

Base schema SHA, extension digests, custom reference DB/DDL lexer and duplicate trigger
verifiers protect exact historical text rather than numeric compatible state. Use
`application_id`, monotonic versions and ordered transactional migrations; bridge
only concrete known legacy states once.

### 19. MCP reserializes multiplex and multiplies polling

`swarm-client::Client` already supports bounded concurrent correlated RPC. MCP wraps
`Option<Client>` in a mutex and holds it across the whole awaited request, so one slow
Store read blocks unrelated tools in that MCP session.

The subscription facade allows 16 subscriptions per session. Each owns a poller,
forwarder, record queue and 1 MiB byte budget, polls `report.delta` every 250 ms and
can scan extra pages to count dropped matching items. From configured constants this
can attempt 64 idle page reads per second per session before backlog scans; all pollers
then serialize on one pump mutex. This is a derived source bound, not a benchmark.

Store already provides durable `report.delta`/`monitor.follow` cursor semantics. MCP
needs one coalesced wake pump, not another replay/gap engine.

### 20. `tasks/get` performs a global attention scan

For one Operation, MCP pages all `report.attention` rows in batches of 200 and filters
locally. The Store API accepts only `after`/`limit`; its implementation builds the
unified attention inventory before pagination. One task read is therefore O(fleet
attention) and also occupies the serialized MCP tool link. Add one exact bounded
Operation/binding projection.

### 21. Ephemeral native streams occupy the Store writer

`module.stream.publish`, `stream.open`, `stream.read` and `stream.close` change/read an
in-memory ring and write no Store domain records, yet `native_streams::call` runs the
whole authorization + ring operation through `Store::run`. High-rate native content
can therefore queue behind or in front of unrelated mutations. Resolve authorization
on a WAL reader and operate the bounded ring outside the writer; revalidate every read
and clear link-owned viewers on disconnect.

### 22. CheckRun still has two execution owners

`CheckConfig.executor` is optional. New work with a pin launches standalone
`swarm-checks`; absence still falls through to host `checks::worker::prepare_and_spawn`
and the hidden root `check-worker`. Reconciliation branches again between
`standalone_host::finalize_execution` and `worker::completion`.

The low-level `swarm-checks::execute_with_plan` is already used by both paths, so the
remaining duplication is bootstrap, worker identity/start files, direct child custody,
packaging and recovery routing—not Check semantics. Require the standalone path for
new CheckRuns and keep legacy receipts as finite readback only.

The hidden host `check-worker` is also used by source/tool-version and Git-hook probes.
That prevents deleting it with the full CheckRun fallback. Move those bounded probes
to the selected check executable first; do not create another probe service.

### 23. ScriptRun has one executor but two wire definitions

New ScriptRuns already reject missing standalone worker pins; `None` survives only to
decode old Work receipts. The hidden host script-worker entrypoint simply calls the
standalone crate.

Host `scripts::runner::Work` and worker `ScriptWork` are documented as intentionally
wire-compatible but are independently defined. Executor pin, start decision,
post-allow plan and completion coordinates are similarly repeated. The result is two
validators and legacy Go/Deny branches around one execution route. Make
`swarm-scripts` the existing domain DTO owner and remove the compatibility entrypoint
and old readers after finite disposition.

### 24. Forge combines two legitimate effects but repeats custody and hashes

Local workspace Git reads/preparation and remote publication are different effects.
Keeping local bounded Git in the host and remote push/readback in
`swarm-forge-worker` is valid. The current Store owner, however, carries both blocking
Git closure custody and standalone native-worker custody in one large file, while the
worker implements another capture/termination loop.

`ForgeExecutionPreparation::capture` hashes both the Forge worker and Git executable
before an effect is admitted. Script and Check launch paths also hash selected worker
binaries; Script retains the pin and the worker verifies it again. These full file
reads repeat package installation checks on every Operation.

Use installed package identity once, OS process identity for custody and content
hashes only at actual immutable byte boundaries. Keep exact target serialization and
remote-ref readback; remove per-effect executable digests and duplicate capture code.

### 25. Existing OS primitives are sufficient

`swarm-process` already owns Group/Job/process birth identity, bounded capture and
private atomic files. Check and Script use them; Forge can use the same physical
primitives. A new universal EffectRunner, workflow or receipt is not justified:
Check coverage/verdict, Script result/effects and Git remote-ref reconciliation are
different domain semantics.

### 26. Automation identity and optional product breadth remain excessive

Automation transfer clones entries and relocates journals because owner/name is part
of identity. Concilium, contract/integration ratification/ACK, automatic publication/
Issue-label projection, document-edition permission and legacy acceptance remain
outside compact-core completion.

### 27. Claude has two storage engines around one live SDK query

The current Claude route has a useful safety core: Rust writes intent before SDK
input, assigns a UUID, correlates SDK frames to the retained Operation, and refuses to
guess after transport loss. The official SDK supplies startup, streaming query,
permission callback/hooks, session identity and session-history reads.

The surrounding implementation is disproportionate:

- `lib.rs` is 234,105 bytes;
- `journal.rs` is 183,134 bytes;
- `interactions.rs` is 118,974 bytes;
- these three Rust files exceed 536 KB before `native_state.rs` and the Node driver.

`OperationJournal` mixes native intent/outcome, Store outbox, result references,
startup inventory/recovery, cleanup/compaction, tombstones, boot markers and an
operation-reference graph. `InteractionJournal` repeats retention state, pending
indexes, tombstones and cleanup cursors for callbacks whose JavaScript resolver cannot
survive bridge loss.

The result path exposes a concrete boundary error. `agent.result` sends
`module.result` directly. If the Store ACK is lost, the adapter records
`MODULE_RESULT_UNKNOWN`; `settle_result_intent_without_replay` then forbids submitting
the same page again. Native prompt replay must remain forbidden, but exact delivery of
an already formed idempotent Store page should be retried until ACK/readback.

The bounded result body is also kept in `NativeControl` memory only. A bridge restart
can lose a known terminal body even though the session/history may still exist. At the
result frame, persist the immutable body/identity before memory loss or use the
supported session-history reader; then deliver exact bytes through the Store-delivery
spool.

Target:

1. one live session/action owner;
2. one minimal `NativeEffectRecord` only until native disposition is known;
3. one exact `StoreDelivery` record for outcome/result until ACK;
4. one current pending interaction projection plus exact reply Operation;
5. no generic tombstone, cleanup, retention or cross-operation reference framework.

Bridge loss retires/unknowns unresolved callbacks; it never replays replies into a
replacement query. Store observations and Operations retain the audit trail.

<a id="code-reduction-candidates"></a>

## Additional reduction candidates

- frontend installation inspects retired sibling trees and duplicate installers;
- qualification treats checkout/compiler/cache path as compatibility;
- host links worker implementation crates;
- most pure reads still use the writer;
- Muse Rust stop can wait indefinitely;
- launch preview hashes and MCP readiness delay direct work;
- route validation hardcodes package IDs/vendor option shapes.

## Claims explicitly rejected

- One MCP exposure catalog is not automatically duplicate authorization; parallel
  routing/authorization ownership is the defect.
- `stream.*` live content is not the same domain as durable `monitor.follow` history.
- Check, Script and Forge do not share one business workflow merely because all spawn
  processes.
- Schemars is not a proven net deletion for current MCP inputs.
- Octocrab is optional; current `gh` is not a giant HTTP implementation.
- Result-page retention before ACK is necessary.
- Transcript, alive PID or mtime does not settle uncertain native writes.
- A Lane entity, ORM, permanent migration framework, broker or universal journal is unnecessary.
- CCCC suspended launch is unnecessary without a proven owner-first race.
- Claude session persistence does not make uncertain prompt/reply replay safe; it is
  a readback source, not an idempotency key.

## Implemented source reduction

Commit `25a8e3b` removed the host's duplicate application CLI/MCP launcher:
`main.rs` fell from 1,949 to 743 physical lines, net −1,206 Rust lines. Later
documentation changes are not runtime deletion.
