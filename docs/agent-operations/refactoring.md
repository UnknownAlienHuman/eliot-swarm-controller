# V3 refactoring map

2026-10-18 · Source recheck at `25a8e3bceaadeefb700defa164a4e401885f7e1c`.
Owner baseline: `f1b854b6fd058dc2440d3848fc95acc279657d3f`, parent
`27fcae1ed2d9fc0aa778680d5e39a08834248023`. This revision changes the reduction
program and documentation; the source mechanisms identified below still exist.

## 1. Authority and audit scope

The compact product is **manager → Task/Attempt → agent execution → attributable
result → independent review → acceptance**, with observation and selected Goal
continuation. Addressed peer messages support that work. Separate council,
interface-ratification and automatic GitHub-label engines are outside its required
completion scope. They are removal candidates, not prerequisites to finish first.

This map owns the active reduction order. V3 documents describe the target; older
O/M/C/I programs, V18, policy editions and module histories supply context, not
cumulative obligations to preserve every experiment. Existing runtime behavior
is evidence to inspect, not a reason to keep it. [Status](../implementation-status.md)
records actual implementation and checks. The [Issue #153 closeout](../remediation/2026-10-15/ISSUE-153-CLOSEOUT.md)
retains the dated audit and corrections; this is a source study, not full qualification.

| Owned Rust scope | Baseline physical lines | Current physical lines | Explicit test-only estimate |
| --- | ---: | ---: | ---: |
| All 479 files, excluding `vendor/` | 534,926 | 533,720 | 172,511 |
| `crates/swarm-kernel-host` | 366,932 | 365,726 | 128,996 |

Counts include comments and blanks. The test estimate follows `tests/`, test-required
`cfg` items and resolvable module descendants, masking comments/literals; it does
not establish Cargo reachability. The current remainder, 361,209 lines, is not
production SLOC. Subsystem sizes below include tests unless stated otherwise and
are inspected surfaces, not promised deletions; overlapping surfaces cannot be added.

## 2. Audit findings already addressed

Do not reopen fixed or disproved hypotheses merely because an old audit names them.
The qualification-only automation fixtures are gated; [acceptance](../../crates/swarm-kernel-host/src/store/acceptance.rs)
loads the source Operation rather than treating a CheckRun ID as an Operation ID;
launcher cursor, result-cache and OpenCode retention-uncertainty corrections exist.
The dead WorkQueue/DemandGate was removed. Shared canonical-JSON/digest helpers
already have owners in `swarm-contracts` and `swarm-scripts`.

Antigravity already associates a serialized turn with its Operation and retains
normalized body pages. Codex/Claude native input identities work differently and
remain useful. Source presence is not a claim that every native scenario passed.
See the MCP and ScriptRun corrections below before assigning another cleanup.

## 3. Current gaps against V3

| Core boundary | Source reality | Required smaller path |
| --- | --- | --- |
| Result collection | OpenCode [native reader](../../crates/swarm-adapter-opencode/src/native.rs) and [result page](../../crates/swarm-adapter-opencode/src/lib.rs) require an explicit assistant parent. | Capture the admitted serialized-turn window and return its body; retain explicit native lineage when available. |
| First dispatch | [Launcher dispatch](../../crates/swarm-kernel-host/src/store/launcher_dispatch.rs) requires current C8 MCP proof for launcher-owned work. | Valid Task/workspace/binding admits the first prompt; native MCP observations arrive asynchronously. Remove the actual admission dependency, not just an evidence-field comparison. |
| Reads | [Store::call](../../crates/swarm-kernel-host/src/store/mod.rs) sends only status/monitor to one FIFO reader; most other reads use the writer queue. | Bounded WAL snapshots, one writer, explicit routing by query family. |
| Capacity | [capacity.rs](../../crates/swarm-kernel-host/src/store/capacity.rs), 5,630 lines: v2 scope headers and separate JSON entry rows in `meta`; v1 whole-ledger reads remain. | Indexed domain rows through the existing schema installer; remove key/codec/orphan plumbing after conversion. The entire reservation subsystem is not expendable. |
| Automation state | [config](../../crates/swarm-kernel-host/src/automation/config.rs) and [Store automation](../../crates/swarm-kernel-host/src/store/automation.rs) store sealed JSON/key families. | Stable entry identity and indexed state; changing owner must not relocate every journal. |

The OpenCode prerequisite is an availability restriction; a circular model/MCP
request deadlock was not established. Its plugin can list tools before a prompt.
Removing that prerequisite still follows the V3 direct-launch target.

## 4. Connected work order

Reduce obsolete mechanisms and optional scope before splitting the remaining
code into more packages. The table is the work order; it does not create a new
Issue series or approval protocol. Independent rows may proceed together.

### Code reduction candidates

| Candidate and source evidence | Decision and replacement |
| --- | --- |
| **Retired installation baggage.** [Frontend installer](../../tools/modules/Install-SwarmFrontend.ps1) enumerates all installed siblings and can rehash retired OpenCode `node_modules` before allowing a new frontend. Its tree/closure helpers alone occupy 174 lines; the [legacy fixture harness](../../tools/qualification/Test-InstallFrontendLegacyCompat.ps1) adds 316. [Forge installer](../../tools/modules/Install-ForgeWorker.ps1) adds 129 lines with no discovered code/CI caller; `Install-StandaloneWorker.ps1` already handles Forge. | **Remove** the retired tree checks, fixture and duplicate Forge installer. An old release directory can remain on disk without its dependencies becoming an admission condition for an unrelated new binary. No alias wrapper. |
| **Build provenance as product compatibility.** [Native qualification](../../tools/qualification/New-NativeQualification.ps1) rejects host/module differences in commit, manifests, compiler strings and even hashed build-cache path (`HOST_MODULE_BUILD_PROVENANCE_DIVERGED`). [Provenance helpers](../../tools/ci/SwarmBuildProvenanceHelpers.ps1) rebuild dependency inventories; builders match private Rust source text with regex. | **Remove** checkout/compiler/cache-path equality and source-spelling checks. Package the selected Cargo target; record optional build facts without making them runtime rights. Installation needs selected files and one descriptor/image contract. Use the shared Rust validator; remove the PowerShell schema copy and redundant permanent copy hashes in [Install-ModuleArtifact](../../tools/modules/Install-ModuleArtifact.ps1). |
| **Retired Python Codex.** Four owned Python files under [modules/codex](../../modules/codex/README.md) total 4,277 lines, plus a vendored SDK tree. [require_new_binding](../../crates/swarm-kernel-host/src/store/task_prompt.rs) rejects its old artifact. Discovered execution callers are fixture/CI/Justfile paths, not the Rust adapter. | **Remove** Python executor, SDK package, old build/test job and launch instructions. The 165-line [receipt decoder](../../crates/swarm-kernel-host/src/runtime/codex.rs) has real Store callers but does not need that Python system. Convert/export actual required records and then retire the decoder too. |
| **Two OpenCode implementations.** Host [runtime/opencode_v2](../../crates/swarm-kernel-host/src/runtime/opencode_v2.rs) subtree: 19,948 lines, plus facade/Store integration; [standalone adapter](../../crates/swarm-adapter-opencode/src/lib.rs): 24,984 across `src/`. [Legacy demand](../../crates/swarm-kernel-host/src/store/legacy_worker_demand.rs) and host workers still execute the built-in route independently of module supervision. | **Converge on the standalone owner.** Move required session/result/recovery consumers, then remove built-in routing, executor and its fixture machinery. Do not keep a second general runtime for archive reads. Fresh-owned start already rejects; code after its unconditional guards is a separate immediate deletion, not a reason to delete still-used readback blindly. |
| **Two Muse implementations.** Node `modules/muse/*.mjs`: 4,298 lines; Rust adapter `src/`: 21,774. Node remains the default route; the Rust template is disabled, with one recorded native exact-result success. | **Choose one owner** after comparing the required consumers and total owned code. Inspect reuse of SDK `Connection`/fold instead of rebuilding native machinery; no third bridge or automatic preference for a larger Rust implementation. Complete the selected path and retire the other executor/journal format through a finite transition. |
| **Two schedulers and legacy worker lifecycle.** [scheduler.rs](../../crates/swarm-kernel-host/src/scheduler.rs) and [automation_scheduler](../../crates/swarm-kernel-host/src/store/automation_scheduler.rs) handle interval, cron and Goal due work. Enabling standalone does not disable built-in demand. [legacy_optional_workers](../../crates/swarm-kernel-host/src/host/legacy_optional_workers.rs) has 11 worker kinds and 2,337 lines. | **One scheduler and lifecycle owner.** Convert used interval definitions into the common entry/due model and remove the old loop/config registry. Store may already suppress duplicate effects; the demonstrated duplication is orchestration. Remove retired worker variants, not just the word `legacy`. |
| **Document editions as permissions and historical acceptance branches.** [policy.rs](../../crates/swarm-kernel-host/src/policy.rs) requires document path/section/SHA equality before scoped feedback. [acceptance](../../crates/swarm-kernel-host/src/store/acceptance.rs), [automation acceptance](../../crates/swarm-kernel-host/src/store/automation_acceptance.rs), reviews and Task code branch on `LegacyV1`. | **One current scoped-manager policy and acceptance contract.** Check caller, assignment, candidate and action directly. Remove document SHA/path/edition as a capability and V1 business-workflow branches after explicit conversion of needed unfinished work. Historical text is evidence, not an executable rights system. |
| **Owner embedded in automation identity.** [automation_transfer::apply](../../crates/swarm-kernel-host/src/store/automation_transfer.rs) clones an entry, retains a disabled former-owner copy, calls six journal relocators plus cron and writes transfer pointers. Its target-conflict check makes A→B→A collide with A's retained entry, by source inspection. | **Stable internal entry ID; mutable owner/revision.** Journal rows reference the entry ID; running Operations retain their original admitting actor. One transaction changes ownership. Delete relocation, successor traversal, cycle detection and historical copies used as identity. See relational conversion below. |
| **Council and interface-negotiation engines.** Concilium: 3,589 lines in three core files, 11 states and 8 methods. Integration DTO/Store: 3,747 lines for offer/requirement, 14 compatibility dimensions and ACKs. They are advisory; [integration](../../crates/swarm-kernel-host/src/coordination/integration.rs) explicitly does not accept Tasks or change assignments. | **Remove from compact core.** Use ordinary independent assignments/reviews and addressed messages. Delete council rounds/digests and generic agreement/ratification machinery with their methods/consumers. Keep participant registration, scoped messaging and review/submission grants currently colocated in [coordination](../../crates/swarm-kernel-host/src/store/coordination.rs). The 12,697-line wider coordination surface is not all removable. |
| **Automatic publication/Issue labels.** [Publication](../../crates/swarm-kernel-host/src/store/automation_publication.rs), [projection](../../crates/swarm-kernel-host/src/store/automation_github_projection.rs) and its automation publication model total 5,371 lines, with separate cursors/watermarks and special on-behalf contexts. | **Defer/remove this automation from required core.** Preserve explicit publication of an accepted candidate through existing Forge/GitHub commands. Project CI can label published work when needed. Remove the automation settings, schedulers and special contexts together; do not count manual Forge as deleted. |

The full automation subtree plus Store automation files is 54,612 physical lines.
Moving all of it from JSON to tables would retain its product complexity. Select
needed actions first; do not rebuild the removed ones in a cleaner generic engine.

### CLI reduction implemented in the preparation branch

[PR #183](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/183) already
contains the earlier CLI implementation: [host main](../../crates/swarm-kernel-host/src/main.rs)
1,949 → 743 physical lines, net −1,206; the 87-line parser-test section is unchanged.
`swarm-cli` owns public commands; host delegates/internal workers remain. This is
separate from the present documentation-only source study, not the scale target.

### MCP hypotheses rechecked

`swarm-contracts::mcp_catalog::exposes_method` already owns exact exposure; the
frontend forwards to it. Descriptive audiences, discovery and actual Store rights
are different facts. Its roughly 230 lines are not duplicate-policy savings.
[Schemars](donor-map.md#12-schema-generation-candidate) has no surveyed shared input
DTO ready to replace the existing manual tool inputs: do not invent 164 DTOs or a
new schema DSL for a cosmetic deletion.

ScriptRun is also not another active default root executor: current reconciliation
rejects queued `executor=None`, and the runner requires a selected standalone.
Its old fallback comment/wire and compatibility entrypoint need cleanup; they do
not prove that new work still runs through the retired backend.

### Retire executors without losing history

Support one current executor and active-work format. A source enum, old fixture,
versioned directory or historical audit is not evidence of an installed consumer.
Do not promise indefinite readers for every prior artifact/SDK/model-default variant.

For concrete data that must survive, use the smallest finite transition: finish or
reconcile identified in-flight work with its current owner, use a supported session
handover, or export required results and convert unfinished Tasks to the current
contract. A semantic change may require a new Task revision/Attempt; keep the old
record as history instead of silently assigning it new rights. Uncertain native
effects are resolved or left explicitly for manual disposition, never resent.
After the transition, remove old producers, readers, schema branches and packaging.
No general migration framework, permanent dual writes or fallback executor is needed.

Keep original dispatch provenance distinct from protocol compatibility and the
currently authorized result reader. [normalized_result::same_module](../../crates/swarm-kernel-host/src/store/normalized_result.rs)
currently compares full artifact identity; [Muse integration](../../crates/swarm-adapter-muse/registration/INTEGRATION.md)
shows the resulting cross-build read restriction. Change that contract deliberately
with an explicit authorized handover/result mapping; do not forge old receipt bytes
or require the old binary forever merely because a build version changed.

### WAL migration detail

Reuse `swarm-store::open_reader` and a bounded connection pool. Authorize and read
one response in the same short read snapshot; retain page ordering/cuts. Start with
status/monitor and bounded catalog/profile/route reads. `module.next` writes dispatch
state; stream methods manage registries; artifact bodies need authorization and
cache-epoch validation before file I/O. Do not route by method suffix alone or keep
a DB transaction open across client pages. Pool saturation uses bounded backpressure.
[SQLite WAL](https://sqlite.org/wal.html) permits concurrent reads/writes; long readers
can delay checkpoints. Measure actual performance after the change.

### Relational conversion detail

Use `Store::initialize_database`, `install_schema_extension` and the existing host
migrations. Store owns domain DDL; `swarm-store` owns connection mechanics. Convert
one bounded family on the writer, validate the saved identities/reservation totals,
switch its consumers and remove the replaced keys/code. Bounded JSON belongs in its
owning row where useful; no table per leaf, second migration runner or ORM is needed.
Held or unknown capacity cannot become free during conversion.

For automation, introduce an immutable internal `entry_id` distinct from its editable
name and `owner_manager_id`. Existing state/occurrences reference that ID. Resolve
actual source/successor entries to one logical entry during conversion; unrelated
same-name entries remain distinct. Ownership changes update owner/revision without
moving state or minting new consumed-occurrence identities. Pending starts check
current authority; already admitted Operations retain original actor and input.
This removes the need for six `relocate_state` paths and transfer-pointer ancestry.
Replace `scoped_entries` key scans/N+1 loads with one indexed owner/project query.

### Remove worker implementation edges

These edges concern retained external executors. Pure functions, planning and
small projections stay in existing libraries. One owner does not imply a new
process, descriptor or IPC protocol for every helper. Consolidate existing
process lifecycles; do not build a new worker framework.

| Host implementation edge | Existing shared owner | Independent remainder |
| --- | --- | --- |
| `swarm-supervisor` | `swarm-contracts` DTOs, `swarm-client` control, `swarm-process` ownership | Registry, spawn/restart and child lifecycle |
| `swarm-bus`, including through `swarm-scripts` | `swarm-contracts` event/selector contract | Dispatcher/wake loop; Store retains durable event/Operation transaction |
| `swarm-script-worker` | `swarm-scripts` plan/wire/start decisions, `swarm-process` primitives | Child execution/capture; remove the host compatibility entrypoint |
| `swarm-checks` | `swarm-contracts` plan/result, `swarm-process` capture | Check execution |
| `swarm-automation` | Shared config/ready facts and process ownership | The single selected scheduler loop |
| `swarm-observer` | `swarm-telemetry` producer/config and shared process facts | Recorder, metrics and observer worker |

Move a used seam with both callers and remove its dependency edge. Do not add empty
wrapper crates, copy workers into the host or claim test relocation as code reduction.

## 5. Compatibility, journals and donor completion

[Donor reuse](donor-map.md) replaces a needed mechanism; it is not a checklist to
add every library/product. Current rmcp/ACP/croner/Atlas integrations remain useful.
Cargo already exposes targets and dependency metadata; an installer need not parse
Cargo.lock or certify the text of a private function. Waku's sequential OpenCode
worker and Paseo's initialization/close barrier are narrow patterns, not extra
workflow engines to import.

Keep the selected adapter's actual command/result persistence with that adapter.
`swarm-process` already supplies JSONL and private durable-file mechanics. Different
journal formats do not justify a universal event-sourcing framework, nor indefinite
maintenance of obsolete formats. Muse SDK `Connection` accepts caller-supplied IDs;
the current Node bridge already passes `maxAttempts:1`. Compare that reuse against
Rust-owned transport before adding a new bridge; the SDK's higher-level `Session`
has its own recovery/replay coordinator. See the inspected [SDK boundary](donor-map.md#9-official-vendor-sdks-and-native-protocol-boundaries).

GitHub API replacement remains a small optional candidate: existing `run_gh_api`
is about 104 lines, not a large HTTP stack. Any octocrab backend keeps current
account/host selection, response bounds and one uncertain-write readback owner.

## 6. Completion and verification

An implementation removes a named mechanism with its obsolete callers/config/tests
and updates its owning contract. Report actual net changes across all owned languages;
compare behavior, not line-count targets alone. Verify the affected behavior and
actual dependency boundary using existing scoped checks. A runtime change needs
appropriate source/runtime evidence; a documentation study needs content/link checks
and no Rust build, toolchain work or native/model campaign. Keep one integrated diff
and one work queue; do not turn every historical failure into another release gate.
