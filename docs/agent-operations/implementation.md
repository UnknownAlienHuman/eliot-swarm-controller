# Eliot Swarm Controller — Implementation

This is the single implementation order for the [architecture](architecture.md).
Source rechecked at `f7e1b634b619dc2c1a723fe357307f7daab12541` on 2026-10-18.
Source descriptions name existing code; implementation instructions are target
changes unless explicitly marked completed.
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
| User surfaces and observation | [swarm-cli](../../crates/swarm-cli/src/lib.rs) owns application CLI mapping; [swarm-mcp](../../crates/swarm-mcp/src/mcp/mod.rs) uses the shared catalog. Adapters collect native data; [host lifecycle](../../crates/swarm-kernel-host/src/host.rs) will own core OS/worktree collection; Store joins the facts. The existing observer recorder and [swarm-telemetry](../../crates/swarm-telemetry/src/lib.rs) own optional diagnostics. |

## Implementation order

Complete one connected change: producer, handler/admission, consumer and obsolete
path removal. Independent rows can proceed with separate file ownership. Each row
below points to existing source; the detailed findings are not another checklist.

| Work | Edit and remove | Result |
| --- | --- | --- |
| Retire unused installation/executor paths | [Frontend installer](../../tools/modules/Install-SwarmFrontend.ps1), legacy fixture, duplicate Forge installer and retired Python Codex identified in [findings](refactoring.md#code-reduction-candidates). Remove their CI/Justfile/config consumers together. | A current installation does not inspect retired siblings; current Codex needs no Python executor or vendored bridge SDK. |
| Remove policy/provenance bureaucracy | [policy.rs](../../crates/swarm-kernel-host/src/policy.rs), `LegacyV1` acceptance branches, [qualification](../../tools/qualification/New-NativeQualification.ps1) and [package helpers](../../tools/ci/SwarmBuildProvenanceHelpers.ps1). Follow [identity rules](#identity-and-hashes). | Caller/object rights and the selected module contract replace document editions and checkout/compiler/cache-path equality. |
| Converge native execution and finish the productive path | Apply [shared harness control](#shared-harness-control), [passive observation](#passive-agent-observation) and [launch/results](#launch-and-results). Move needed built-in OpenCode consumers to the [standalone adapter](../../crates/swarm-adapter-opencode/src/lib.rs); remove host native execution/routing. Compare Muse Node/Rust behavior and total owned code, select one and retire the other. | Common commands, independent native/Eliot Goals and a local process/workspace/session view. Remove vendor branches in plugin delivery, artifact-version usage allowlists and duplicate Muse normalization. No third bridge or pre-prompt MCP gate. |
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

## Shared harness control

Implement the [common command meanings](module-api.md#common-harness-commands) in
existing `swarm-contracts::module_command`, `runtime` and `mcp_catalog`; update the
Store handler, CLI/MCP and selected adapters together. Do not add a generic native
RPC tool, provider-specific manager prompts or a second session framework. Keep
one small declaration of supported actions on the selected adapter/session. Model
and mode choices come from its existing catalog; runtime-name switches in callers
are not capability handling. An unsupported action fails before native I/O.

### Commands and the bus

Store admits an Operation with its binding/generation and retained Task context.
[ModuleLink](../../crates/swarm-client/src/module_link.rs) carries `module.next`,
`module.outcome`, `module.observe`, stream and result traffic; it owns no work policy.
The adapter maps the command and folds native events through one connection owner.
Keep that reader/control pump responsive while a turn runs, including pending
approval replies and cancellation. A command can finish on native input admission;
waiting for its eventual output must not block the next control command.

Use the existing Store observation journal for durable facts, with cursor and
selected follow-up admitted in the same transaction. Notifications wake readers;
bounded live streams carry display deltas. [swarm-bus](../../crates/swarm-bus/src/lib.rs)
is already a pure metadata selector router. Keep its needed matching function in
an existing owner when removing the host's worker dependency. Store
[bus admission](../../crates/swarm-kernel-host/src/store/automation_dispatch_bus.rs)
already owns journal paging and atomic cursor/action disposition. No broker,
second event log or message per token is required. Addressed collaboration messages
keep their existing mailbox semantics; they do not transport native commands.

Native inboxes own their queue/steering order. Eliot retains admitted Operations
and native item handles, not a second scheduling copy. Add the planned `agent.cancel`
handler: unsent input uses [operations::cancel](../../crates/swarm-kernel-host/src/store/operations.rs);
native turn/queued-input cancellation uses the adapter. Preserve separate native
terminal/readback evidence, including no-run terminals; an unknown cancellation
preserves the original work. [Donor mappings](donor-map.md#9-official-vendor-sdks-and-native-protocol-boundaries)
identify the native calls and their scope.

### Harness Goals and the Eliot Goal plugin

Keep the [two mechanisms](module-api.md#goal-continuation) independent. The Governor
uses native Harness Goal by default where supported, and may enable Eliot Goal
messages alongside it. There is no exclusive driver field or driver-switch protocol.

**Harness Goal:** keep `agent.goal` as binding-scoped native control. The adapter
maps set/edit/pause/resume/clear and reports native state through `agent.state`.
It accepts the Governor's objective without requiring an Eliot Goal record. For
assigned work, retain Task/Attempt context with the initiating Operation. Set/resume
may start a native turn: use the existing TaskPrompt context where needed, map the
resulting turns to that Operation, and extend [result selectors](../../crates/swarm-kernel-host/src/store/results.rs).
Do not accidentally dispatch the initial assignment twice. Later intentional
messages remain ordinary inputs. A native completion claim does not accept the Task.

**Eliot Goal:** retain the Task-scoped records in Store
[goals](../../crates/swarm-kernel-host/src/store/goals.rs). Implement its message plugin
as a small action in the existing automation owner: selected event or due occurrence
→ current Goal/assignment → message → `agent.send`. Reuse the scheduler's due state
and the existing command bus; no interpreter, separate process or timer per Goal.
The Governor selects message content, trigger/cadence and delivery mode. New and
repeated text use the same next-turn/steer handler as a manual message.

Refactor [automation_goal_progression](../../crates/swarm-kernel-host/src/store/automation_goal_progression.rs)
through its callers. Today it is terminal-event-only, selects Codex `agent.send`
versus OpenCode `agent.goal continue`, and repeats that choice in validation.
[GoalProgressionSettings](../../crates/swarm-kernel-host/src/automation/config.rs)
contains only `goal_id`; recurring message policy is not implemented. Extend that
small plugin configuration using existing event/interval scheduling, and change
[action admission](../../crates/swarm-kernel-host/src/automation/actions.rs) to require
message delivery. Remove the OpenCode pseudo-Goal path, provider-specific continuation
contexts and objective approval hash when these callers move. Keep native Goal
revision handling solely in its adapter.

Commit each scheduled/event occurrence with its message Operation. Replaying that
occurrence returns its original Operation; a later scheduled occurrence may carry
the same text. After downtime, coalesce missed ticks instead of flooding the native
inbox. Preserve uncertain sends for readback. Disabling Eliot Goal stops its future
messages; pausing/clearing Harness Goal affects that native goal. The Governor can
request both explicitly. Neither action implicitly cancels turns or queued input.

### Passive agent observation

Implement the [local view](observability.md#5-live-monitoring-and-future-charts)
through existing owners. Observation collection is independent of automation,
diagnostic logging and viewer count; a status read makes no harness request.
The current `swarm-observer::host::HostRecorder` is only an optional diagnostic
sink. Add one bounded local collection task to [host::run_until](../../crates/swarm-kernel-host/src/host.rs),
with host startup/shutdown, outside `observability.enabled` and automation entries.
Reuse process sampling primitives and the host-only status-delivery pattern in
[module_supervisor_observation](../../crates/swarm-kernel-host/src/store/module_supervisor_observation.rs);
keep its needed types/helpers in existing shared owners as worker dependencies
are removed. This core collection lifecycle is not implemented today.

1. At launch/resume/attach, retain the native session and available local source
   reference beside the binding. Join the existing Task/Attempt/Operation to the
   [launch workspace lease](../../crates/swarm-kernel-host/src/store/launcher.rs):
   repository/worktree, path, assigned branch and baseline. Reuse
   [process ownership](../../crates/swarm-process/src/module_owner.rs) and supervisor
   observations for PID plus OS birth identity. A shared server can host many
   sessions; native children need no invented process of their own.
2. In the selected adapter, fold native events and read available local provider
   JSON/JSONL/SQLite independently of its connection to the provider harness.
   Keep the separate Eliot ModuleLink alive while native control reconnects. Use the
   [vendor readers](donor-map.md#local-observation-sources), one bounded collector
   per store/service scope. Tail from a retained file identity/offset; keep an
   incomplete final record for the next read. Rotation/truncation produces a visible
   gap. For OpenCode, revisit known open messages as well as new sequences: their
   JSON can change in place. Use short read-only SQLite transactions with WAL
   visibility; never run vendor migrations, checkpoints or repair.
3. Fold compact native facts in [runtime::observe](../../crates/swarm-kernel-host/src/store/runtime.rs)
   and expose them through [operations::get_binding_public/public_observation](../../crates/swarm-kernel-host/src/store/operations.rs).
   Adapter facts arrive through authenticated `module.observe`; host OS/worktree
   facts use the host-only status callback, without needing a live adapter boot.
   Keep source time/cursor and coverage per source; an OS or quota update cannot
   refresh old work activity. Move native interpretation, including the current
   OpenCode-only `ses` failure-ID validation, out of this shared projection.
   Expose relevant process/workspace/session references to authorized readers
   without exposing credentials or private launch configuration.
4. The core host task collects process and local Git facts independently. Read
   branch/HEAD/dirty state once per active worktree; retain the
   last known GitHub head from the existing integration separately. Keep bounded
   change/event collection with a coalesced technical tick in that task, independent
   of optional automation, so lost notifications do not freeze the view. No full transcript scan per tick,
   per-viewer poller, new observer database or signed observation envelope.

Retain the source's host/runtime/session and known local data location for bounded
Governor inspection; resolve source references within that scope, without exposing
launch credentials or creating a general filesystem RPC. Discovery by name/cwd is a hint until correlated;
unmatched processes or native sessions remain visible with an unknown assignment.
Native connection loss does not stop local parsing. If the adapter or its Eliot
link fails, Store retains the last native snapshot with a gap while core OS/Git
collection continues; reconnection resumes local-source reconciliation. No claim
of fresh native data follows from that retained snapshot. New code must not
interpret an old provider `running` field as current OS
liveness, or turn observation into automatic ownership, cancellation or restart.

### Shared observation and quotas

Fold activity into the existing `agent.state` projection and quota data into
`NativeUsageSnapshot`; use [their distinct meanings](module-api.md#activity-and-usage-reads).
Keep native payload parsing/units and sparse-patch folding in the adapter. Shared
contracts validate normalized bounds; Store validates the authenticated binding,
boot, selected capability and observation order. Replace
[native_usage::parse](../../crates/swarm-kernel-host/src/store/native_usage.rs)'s
hardcoded artifact list: it currently rejects `muse-rust-controller.1` although
that adapter emits this DTO, aborting the containing state observation.

Generalize this existing producer boundary for Claude rather than adding another
quota DTO or accumulating bridge-version exceptions. Keep exact source scope and
last verified limit state; malformed optional usage must not suppress independent
activity/result delivery. An event reporting a limit can reuse
[ProviderCondition](../../crates/swarm-contracts/src/provider_condition.rs), without
pretending it supplied utilization counters. OpenCode account quota remains
unavailable where its selected native surface provides none.

Collect quotas on connection and native changes; coalesce requested safe refreshes.
Share account reads only when their actual auth/quota scope is known, and never
sum copies of one account snapshot across agents. Remove duplicate Muse Node/Rust
normalizers with the discarded executor. Cached reads, partial data and a visible
collection issue suffice; do not scrape dashboards or scan transcripts to invent
a missing quota percentage. Local transcript observation remains a separate required
path. Optional experimental vendor reads
stay inside the adapter and cannot become a launch prerequisite.

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

Use the existing [result handoff](module-api.md#results-and-artifacts): `agent.result`
requests a page, the adapter publishes it with `module.result`, and `artifact.read`
reads committed bytes. Store publishes the immutable file before committing its
receipt; these are ordered writes, not one filesystem/SQLite transaction. Keep
the adapter's exact pending page until acknowledgment. A crash can leave an orphan
file, not a completed result. Use `artifact.assemble` for multiple pages; submission
already rejects incomplete coverage. Do not build another result reader/assembler
or remove the lost-ack journal merely because both owners temporarily retain bytes.

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
The [Eliot Goal plugin](#harness-goals-and-the-eliot-goal-plugin) serves the existing
assignment through ordinary messages and this scheduler. Harness Goal keeps its
independent native lifecycle.

Local delivery follows [delivery.md](delivery.md): submission A, independent
review, manager return, correction B and fresh review before acceptance. Preserve
candidate-specific authorship and late results. Remove document-edition rights and
historical acceptance workflows through the finite transition, not by treating old
verdicts as permission for new candidates.

Fix acceptance in the common [Store evidence gate](../../crates/swarm-kernel-host/src/store/acceptance.rs),
already called at reserve, begin and finish. Today direct `task.accept` validates
the requirement IDs in caller-authored `reviews`; only the
[automatic producer](../../crates/swarm-kernel-host/src/store/automation_acceptance.rs)
requires a retained assigned pass. Move that prerequisite to the common gate.
Resolve required [assignments/results](../../crates/swarm-kernel-host/src/store/reviews.rs)
for the exact current candidate and frozen requirements, validate independent
authorship, complete coverage and passing checks, and retain their references in
the acceptance Operation. Recheck that same evidence at finish; do not silently
substitute a newer review. Keep the manager's decision/reason; remove the duplicate
review-proof payload and its construction/validation in the helper. Preserve that
helper's trigger selection and current automation authority checks. A required missing,
inconclusive or changes-requested review blocks both manual and automatic acceptance;
review A cannot satisfy corrected B. Derive eligibility from these records, without
adding an `audited` ledger or a second review engine.
Update `AcceptRequest`, kernel coverage validation, catalog and both callers together.

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

For the selected Muse path, distinguish pending `userInput/cancel`, native turn
cancellation and process shutdown. Dropping a request or flushing its frame proves
none of the latter outcomes. The Rust `RunOwner::join` currently awaits
`Driver::stop_native` → `MspTransport::close` before custody join, with no bound on
pipe backpressure; the custodian can also await child/stderr indefinitely. Put one
bounded stop wait in the existing process owner. On expiry retain custody and a
visible pending reason; continue observation. Only an explicit authorized stop may
use verified owned-family termination. Current Muse custody alone supplies no such
family-stop API: do not claim a timeout or dropping the handle implements it.
Reuse the [SDK submission-barrier distinction](donor-map.md#9-official-vendor-sdks-and-native-protocol-boundaries),
not another shutdown/replay coordinator.

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

Diagnostic simplification stays in `swarm-telemetry`/`swarm-observer`. Their current
bounded Producer/HostRecorder is not a tracing integration. Replace its plumbing
through the existing callers when adopting tracing, preserve byte limits and scope
expiry, and delete the old path. Keep small source-specific records and real IDs;
the [observability contract](observability.md#3-common-record-and-causal-relationships)
does not require a universal event DTO, another configuration service or a second
copy of Store evidence.

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
| Shared harness control | The same controller request reaches different native mappings; unsupported native actions are explicit. Cancellation distinguishes turn, queued input and surviving work. Current Muse Rust usage reaches Store; unavailable/malformed quota does not erase valid activity or become zero. |
| Two Goals | Native Goal works alone by default where supported; the Eliot plugin works alone or alongside it. Two scheduled occurrences may send the same text, while replay of one occurrence reuses one Operation. Plugin disable and native pause affect only their own mechanism; neither cancels active work implicitly. |
| Passive observation | Disconnect the native control connection and read process/worktree/provider-local facts without a harness request. A collector failure preserves last facts with a gap. Cover a shared server with two managers, PID reuse, branch change, mutable open message, partial/rotated JSONL and an unmatched session; none creates a false owner or terminal result. |
| Launch/result | Direct launch needs no preview token; stale revisions/changed workspace are rejected; ready session dispatches without MCP discovery; explicit-parent and complete serialized-turn output are attributable; an actual gap stays unresolved without replay. |
| Delivery/automation | Manual/helper share the evidence gate: a required missing/changes-requested review fails; B needs B's pass, while checks-only policies remain possible. Disable/start ordering, late results and A→B→A ownership preserve the right work. |
| Store | Restart/lost notification retains an action; reader snapshot is authorized and bounded; migration preserves identity and held capacity; cosmetic SQL text does not define database compatibility. |
| Installation/process | Selected files suffice without retired siblings or identical build directories; adapter changes leave unrelated host execution intact. A blocked close returns a pending wait with custody retained, not false completion; actual ownership controls explicit stop/replacement. |
| Frontends and deletion | CLI/MCP reach the same handler; obsolete callers/config/tests/dependencies are removed, with no fallback wrapper. |

Record what ran and the actual net owned-code change, separating tests. Source,
fixtures and native execution are different evidence. Update existing status;
a documentation change requires content/link checks, not runtime qualification.
