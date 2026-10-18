# Eliot Swarm Controller — Implementation

This is the single implementation order for the [architecture](architecture.md).
Source rechecked at `4e75fb1fdfc6b9cd4aee77f21c9b68dcc61761c5` on 2026-10-18;
the mechanisms below are present unless explicitly marked completed.
[Findings](refactoring.md) retain source evidence and counts;
[status](../implementation-status.md) records execution evidence. O/M/C/I labels
in older documents are historical references, not additional work queues.

## Code ownership

Use the existing owners. Move a needed contract to its shared owner with its
callers; do not create a crate or service for every row.

| Responsibility | Source owner and implementation boundary |
| --- | --- |
| Durable work and rights | [Host Store](../../crates/swarm-kernel-host/src/store/mod.rs): admission, Tasks, Operations, reservations, events and domain DDL. [swarm-kernel](../../crates/swarm-kernel/src/lib.rs): existing actor and pure policy. [swarm-store](../../crates/swarm-store/src/lib.rs): connection/transaction mechanics. These are libraries inside the host. |
| Shared wire and IPC | [swarm-contracts](../../crates/swarm-contracts/src/lib.rs): DTOs, method policy, module catalog. [swarm-client](../../crates/swarm-client/src/lib.rs): bounded authenticated IPC and ModuleLink. Shared libraries do not import the host or adapters. |
| Task delivery | Store [tasks](../../crates/swarm-kernel-host/src/store/tasks.rs), [submissions](../../crates/swarm-kernel-host/src/store/submissions.rs), [reviews](../../crates/swarm-kernel-host/src/store/reviews.rs) and [acceptance](../../crates/swarm-kernel-host/src/store/acceptance.rs) own the existing transitions; [delivery](delivery.md) owns their behavior. |
| Native execution and results | The selected `swarm-adapter-*` owns vendor transport, session/turn mapping and native recovery. Store [normalized_result](../../crates/swarm-kernel-host/src/store/normalized_result.rs) and [results](../../crates/swarm-kernel-host/src/store/results.rs) validate and expose attributable results. No second Task store in an adapter. |
| Scheduling and processes | [swarm-automation](../../crates/swarm-automation/src/lib.rs) is the target scheduler service; Store owns due state and admission. [swarm-supervisor](../../crates/swarm-supervisor/src/lib.rs) owns retained worker lifecycles through [swarm-process](../../crates/swarm-process/src/lib.rs). Consolidation is still required. |
| User surfaces and observation | [swarm-cli](../../crates/swarm-cli/src/lib.rs) owns application CLI mapping; [swarm-mcp](../../crates/swarm-mcp/src/mcp/mod.rs) uses the shared catalog. [swarm-telemetry](../../crates/swarm-telemetry/src/lib.rs) owns common diagnostic primitives; observers project existing state without starting models. |

## Implementation order

Complete one connected change: producer, handler/admission, consumer and obsolete
path removal. Independent rows can proceed with separate file ownership. Each row
below points to existing source; the detailed findings are not another checklist.

| Work | Edit and remove | Result |
| --- | --- | --- |
| Retire unused installation/executor paths | [Frontend installer](../../tools/modules/Install-SwarmFrontend.ps1), legacy fixture, duplicate Forge installer and retired Python Codex identified in [findings](refactoring.md#code-reduction-candidates). Remove their CI/Justfile/config consumers together. | A current installation does not inspect retired siblings; current Codex needs no Python executor or vendored bridge SDK. |
| Remove policy/provenance bureaucracy | [policy.rs](../../crates/swarm-kernel-host/src/policy.rs), `LegacyV1` acceptance branches, [qualification](../../tools/qualification/New-NativeQualification.ps1) and [package helpers](../../tools/ci/SwarmBuildProvenanceHelpers.ps1). Follow [identity rules](#identity-and-hashes). | Caller/object rights and the selected module contract replace document editions and checkout/compiler/cache-path equality. |
| Converge native execution and finish the productive path | Move needed built-in OpenCode consumers to the [standalone adapter](../../crates/swarm-adapter-opencode/src/lib.rs); remove host `runtime/opencode_v2` execution and legacy routing. Compare Muse Node/Rust required behavior and total owned code, select one and retire the other. Apply [launch/result changes](#launch-and-results) to the selected implementation. | One executor per retained runtime returns attributable output without a pre-prompt MCP gate. No third bridge. |
| Remove optional product engines | [Concilium](../../crates/swarm-kernel-host/src/store/concilium.rs), [integration](../../crates/swarm-kernel-host/src/store/integration.rs), automatic [publication](../../crates/swarm-kernel-host/src/store/automation_publication.rs)/[GitHub projection](../../crates/swarm-kernel-host/src/store/automation_github_projection.rs), their methods, config and schedulers. | Ordinary assignments, messages and independent review serve collaboration. Explicit accepted-candidate publication remains an optional command. Retain participant registration and review/submission grants colocated in coordination code. |
| Unify automation and indexed state | [automation_transfer](../../crates/swarm-kernel-host/src/store/automation_transfer.rs), Store automation families, [scheduler](../../crates/swarm-kernel-host/src/scheduler.rs) and [automation_scheduler](../../crates/swarm-kernel-host/src/store/automation_scheduler.rs). Apply [stable identity](#automation-and-delivery) and [Store conversion](#store-and-current-data). | One entry identity, scheduler and durable due model; delete journal relocators, successor ancestry, old loop/config registry and replaced `meta` key codecs. |
| Finish read and package boundaries | `Store::call`, [legacy_optional_workers](../../crates/swarm-kernel-host/src/host/legacy_optional_workers.rs) and affected manifests. Use [Store reads](#store-and-current-data) and [process boundaries](#processes-and-frontends). | Bounded independent reads; host stops linking retained worker implementations. Existing public CLI deletion stays completed. |

### Current format and existing data

Backward compatibility is not a product requirement. A historical enum, fixture or
release directory does not justify a converter. Identify concrete required saved
work before writing transition code. Finish/reconcile it with its current owner,
use a supported handover, or export required results and convert unfinished Tasks.
A changed meaning needs a new Task revision/Attempt; preserve the old record as history.
Then delete old producers, readers, schema branches and packages. Disposable
experimental state needs no supported migration path; live state is not silently reset.

## Launch and results

Make launch preview an optional read-only view. [LaunchRequest::parse](../../crates/swarm-kernel-host/src/launcher.rs)
currently requires `plan_digest`; Store [launch_for_actor](../../crates/swarm-kernel-host/src/store/launcher.rs)
recomputes it, and workspace/lease/dispatch consumers repeat the comparison.
Admit explicit settings and expected revisions directly, resolve and retain one
plan under its launch Operation, and reference that record from workspace and
dispatch. Remove the caller-supplied preview hash and redundant plan copies, while
preserving the link between admitted inputs, prepared workspace and dispatched
input. Revision/authority checks remain at admission and the relevant start boundary.

In [launcher_dispatch.rs](../../crates/swarm-kernel-host/src/store/launcher_dispatch.rs),
`require_current_dispatch_capability` still gates dispatch/coalesced dispatch on
OpenCode/Muse MCP connection evidence. Change those admission callers: prepared
workspace + authorized current Task/binding + ready native session admits the input.
Keep actual session/turn validation, including Antigravity's binding evidence.
Tool discovery reports asynchronously. Remove the MCP prerequisite itself rather
than hiding an `awaiting_native_mcp` field.

The OpenCode [native reader](../../crates/swarm-adapter-opencode/src/native.rs)
currently rejects a missing assistant `parentID` in `validate_assistant_parent`.
Implement result association in the selected adapter before deleting the old reader:

1. When the native protocol supplies an input/parent ID, verify and retain it.
2. Otherwise retain Operation, binding/generation, native session, stream generation
   and turn baseline before input; serialize independently attributable inputs in
   that window and retain its scoped body until the matching terminal event.
3. Return the body through the existing normalized-result page contract. A complete
   window establishes association without repeated parent IDs. A second independent
   input, unclassified stream gap or changed generation requires readback of that
   input, never a resend or a guess from the newest assistant message.

Preserve native order and final-body replacement of deltas. Report missing/truncated
bytes or unavailable retained history precisely; a digest cannot reconstruct output.
Result retrieval does not accept a Task. The [Waku/Paseo patterns](donor-map.md#8-architectural-donor-patterns)
simplify execution/lifecycle ownership; Eliot supplies this Operation mapping.

## Store and current data

Keep one serialized writer. Admission, current-rights checks, reservation, state
change and durable event/cursor disposition commit together. Native, Git, file and
network I/O run outside short database transactions.

`Store::call` currently separates status/monitor reads but sends most ordinary
reads through the writer. Reuse `swarm-store::open_reader` with a bounded WAL pool.
Route pure SQL query families explicitly: authorization, page data and journal cut
share one short read snapshot. Do not classify by a `get`/`list` suffix: `module.next`
mutates dispatch, streams own registries, and artifact bodies cross DB/cache/file
boundaries. Do not hold a snapshot across client pages; bound pool backpressure.

Use existing `initialize_database` / `install_schema_extension` in Host Store for
bounded transactional conversion. Index queried identity, owner, state, due and
relationship fields in ordinary tables. Bounded payload/config JSON stays in its
owning row where useful. Convert a used family, validate retained identities and
capacity totals, switch consumers and remove old keys/code. No ORM, EAV schema,
table per JSON leaf or second migration runner. Held/unknown capacity stays held.
Persist a finalized body once per actual owner and reference it; required raw native
ACK/recovery bytes and normalized/redacted output can have distinct lifetimes.

[swarm-store](../../crates/swarm-store/src/lib.rs) currently verifies the base SQL's
SHA before calling the host initializer, and again when opening a reader. Remove
that source-text equality gate. Keep application identity, explicit schema version
and the existing transactional extension installer. Today `user_version` is fixed
at 1, not a migration sequence. Implement each actual schema change there and
validate the affected tables/columns in that conversion. No general base-schema
validator already exists; neither the current stored digest nor a version number
proves every table is intact. SQL whitespace is not a new file format, and this
change does not require a new schema-validation framework.

## Automation and delivery

Give each retained automation an immutable internal `entry_id`, separate from name
and `owner_manager_id`. Journal/occurrence rows reference it. Convert actual
source/successor records to the same logical entry; unrelated same-name entries
remain distinct. `automation_transfer::apply` then updates owner/revision in one
transaction. Delete its six `relocate_state` paths, cron relocation, historical
identity copies and transfer-pointer/cycle machinery. A→B→A must preserve state and
consumed occurrences. Admitted Operations retain original actor/input; pending
starts check current authority.

Keep one scheduler service. Reuse [scheduler/calendar.rs](../../crates/swarm-kernel-host/src/scheduler/calendar.rs)
(`croner` plus `chrono_tz`) for calendar evaluation. Store owns enabled state,
due identity, overlap/catch-up and action disposition. Convert used interval
entries once, then remove the built-in loop and its configuration/demand registry.
Enabling standalone currently does not remove built-in orchestration.

A manual action needs no automation object. Enabled helpers call the same handlers
with a Store-derived manager context. A cursor advances atomically with its action
or retained pending reason; notifications only wake readers of committed work.
Use domain uniqueness for the same candidate/action/occurrence across manual and
automatic callers. A new request ID or config revision does not create new work.

Settings affect future admissions; an admitted action retains its inputs. Start
checks serialize with disable. Disable-first holds unstarted work; start-first can
finish and report its result. Separate follow-ups check current eligibility again.
One continuation owner serves an assignment. Goal references existing Tasks and
completion evidence; it does not own a second task graph or timer system.

Local delivery follows [delivery.md](delivery.md): submission A, independent
review, manager return, correction B and fresh review before acceptance. Preserve
candidate-specific authorship and late results. Remove document-edition rights and
historical acceptance workflows through the finite transition, not by treating old
verdicts as permission for new candidates.

## Identity and hashes

Use ordinary IDs/revisions and field comparisons for local relationships and edits.
Retain a digest only where a consumer actually needs exact-byte identity or a secret
verifier. The [source findings](refactoring.md#identity-machinery) distinguish the cases.

| Boundary | Implementation decision |
| --- | --- |
| Local configuration/launch preview | Keep effect-free preview optional. `automation::apply` already checks expected revisions and validates the patch; remove its extra `preview_digest` comparison. Launch uses the retained Operation plan described above, with no caller-carried approval hash. |
| Documents, build provenance and local diagnostics | Remove policy-document SHA/path/edition admission and checkout/compiler/cache equality. Diagnostic rows use existing source/row IDs and revisions; do not add attestations or agreement hashes. |
| Requests, credentials and immutable bytes | Keep same-request/same-input checks, private credentials, selected executable verification and necessary artifact/native-payload digests. Use existing canonical/byte helpers according to the actual contract; no new hash registry. |
| Participant issuance | `participant_credentials::issue_for_launch` must reuse the same private token after a lost registration reply. Consolidate duplicate manifest/reference-index fields and the two identical indexes into one authoritative record with resolving refs; preserve request/registration binding and exact credential/profile readback. |
| Results across executor replacement | `normalized_result::same_module` currently compares full artifact identity. Keep original dispatch provenance and implement a supported authorized handover/result mapping for required work; do not forge receipts or retain an old executable indefinitely. |

Selected package files and one shared descriptor/image validator are sufficient
for installation. Remove PowerShell's duplicate schema validator, private Rust-source
regex checks and repeated copy-provenance records. Keep actual adapter resources,
including required Antigravity Node files. Cargo metadata supplies target/dependency
facts when needed; it is not another package approval registry.

## Recovery and progress

One owner records an effect's start and confirmed/uncertain outcome. On reconnect,
reconcile that Operation/native input before further input on the ambiguous scope.
Do not give middleware, adapter and scheduler independent uncertain-write retries.
Supported safe reads can use bounded retry; an unknown external write stays unknown
until readback or explicit disposition. Observation and authenticated late-result
ingestion continue after disable or supersession, without authorizing a new action.

Use committed cursors and indexed pending work for restart and wakeup. A lost signal
must not lose an admitted action. A malformed optional record or slow viewer is
isolated to its source/subject; Store failure is reported as Store failure. Keep
streams bounded, redact complete logical records before persistence/fanout, and
show a gap when content is unavailable. No per-token durable event or per-agent
model/status poller is needed.

## Processes and frontends

Remove host implementation dependencies on `swarm-supervisor`, `swarm-bus`,
`swarm-script-worker`, `swarm-checks`, `swarm-automation` and `swarm-observer` after
moving their used contracts/pure functions to the existing shared owners. Keep
Store admission/event transactions in the host. `swarm-process` already supplies
process ownership, private durable files and JSONL mechanics. One lifecycle owner
does not require a new worker framework or process per helper.

`swarm-cli` already owns the public application mapping after commit `25a8e3b`;
the host retains daemon/local delegates and internal workers. Do not assign that
deletion again. MCP uses `swarm-contracts::mcp_catalog::exposes_method` and the same
Store handlers. Small eager surfaces and deferred schemas remain; inventing new
input DTOs solely to adopt Schemars or a generic execute-method tool adds work.

## Donor choices

[donor-map.md](donor-map.md) owns inspected sources and limits. Reuse current rmcp
for MCP transport, `agent-client-protocol` for Command ACP wire types, croner for
calendar evaluation and Atlas for stateless redaction of supplied logical records.
They are actual integrations. Waku's sequential worker and Paseo's initialization/
close barrier are source patterns, not installed Eliot runtimes.

For Muse compare the existing Node bridge and Rust MSP implementation against
low-level SDK `Connection`/event-fold reuse. The current Node bridge already supplies
command IDs and `maxAttempts: 1`; high-level SDK `Session` introduces another recovery
coordinator. Select the path that removes total Rust + JS + glue; add no third bridge.
GitHub currently uses `gh`. Keep it unless a retained requirement and net reduction
justify replacing that transport with octocrab. Adding octocrab is not completion.

## Optional integrations

Hooks record authenticated bounded facts and return; enabled actions run outside
the callback. Scripts use the existing selected runner, immutable invocation inputs,
bounded output and owned descendants. Process groups are lifecycle control, not an
OS sandbox. GitHub publication uses the existing accepted-candidate path and one
remote-effect owner. Local delivery remains usable without any of these integrations.
Use [configuration](configuration.md), [module API](module-api.md) and runtime guides
for their concrete contracts; do not expand them to complete an old capability list.

## Verification

Use existing scoped checks for the changed behavior and dependency closure.
Choose relevant cases rather than turning this table into a new release program.

| Changed boundary | Meaningful check |
| --- | --- |
| Launch/result | Direct launch needs no preview token; stale revisions/changed workspace are rejected; ready session dispatches without MCP discovery; explicit-parent and complete serialized-turn output are attributable; an actual gap stays unresolved without replay. |
| Delivery/automation | Manual work and selected helper share one action; disable/start ordering, correction A→B, late result and A→B→A ownership preserve the right work. |
| Store | Restart/lost notification retains an action; reader snapshot is authorized and bounded; migration preserves identity and held capacity; cosmetic SQL text does not define database compatibility. |
| Installation/process | Selected files suffice without retired siblings or identical build directories; adapter changes leave unrelated host execution intact; actual process ownership controls replacement/cancellation. |
| Frontends and deletion | CLI/MCP reach the same handler; obsolete callers/config/tests/dependencies are removed, with no fallback wrapper. |

Record what ran and the actual net owned-code change, separating tests. Source,
fixtures and native execution are different evidence. Update existing status;
a documentation change requires content/link checks, not runtime qualification.
