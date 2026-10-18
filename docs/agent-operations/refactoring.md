# Refactoring — Source Findings

Source rechecked at `4e75fb1fdfc6b9cd4aee77f21c9b68dcc61761c5` on 2026-10-18.
Owner baseline: `f1b854b6fd058dc2440d3848fc95acc279657d3f`, parent
`27fcae1ed2d9fc0aa778680d5e39a08834248023`. The runtime is unchanged since
`25a8e3b`; subsequent preparation commits change documentation.

This file records why current mechanisms need reduction. [Architecture](architecture.md)
owns purpose and scope; [implementation](implementation.md) owns the only work
order, replacements and transition details. [Status](../implementation-status.md)
records actual execution evidence. Older O/M/C/I lists and the
[Issue #153 closeout](../remediation/2026-10-15/ISSUE-153-CLOSEOUT.md) retain their
historical scope; source presence is not a requirement to finish every subsystem.

## Source size

| Owned Rust scope | Owner baseline, physical lines | Current physical lines | Explicit test-only estimate |
| --- | ---: | ---: | ---: |
| All 479 files, excluding `vendor/` | 534,926 | 533,720 | 172,511 |
| `crates/swarm-kernel-host` | 366,932 | 365,726 | 128,996 |

Counts include comments and blanks. The test estimate follows `tests/`, test-required
`cfg` items and resolvable module descendants with comments/literals masked; it does
not establish Cargo reachability. The 361,209-line remainder is not production SLOC.
Subsystem counts below include tests unless stated otherwise. They are inspected
surfaces, not promised deletions; overlapping surfaces cannot be added.

## Current source gaps

| Boundary | Source observation | Implementation owner |
| --- | --- | --- |
| First dispatch | [launcher_dispatch](../../crates/swarm-kernel-host/src/store/launcher_dispatch.rs) calls `require_current_dispatch_capability` at admission/coalesced dispatch; OpenCode/Muse branches require MCP connection evidence. | [Launch and results](implementation.md#launch-and-results) |
| Result collection | OpenCode [native reader](../../crates/swarm-adapter-opencode/src/native.rs) rejects missing assistant `parentID`; the standalone path has no implemented admitted-turn-window alternative yet. An actual circular MCP/model deadlock was not established. | [Launch and results](implementation.md#launch-and-results) |
| Reads | [Store::call](../../crates/swarm-kernel-host/src/store/mod.rs) separates status/monitor but routes most ordinary reads through the writer. Streams, module dispatch and DB/file composites have distinct stateful paths. | [Store and current data](implementation.md#store-and-current-data) |
| Capacity/state | [capacity.rs](../../crates/swarm-kernel-host/src/store/capacity.rs) has 5,630 lines: v2 scope/header and JSON entries in `meta`, plus v1 whole-ledger reads. Automation uses sealed JSON/key families. The required reservation behavior is not all expendable. | [Store and current data](implementation.md#store-and-current-data) |

## Code reduction candidates

These observations support the existing implementation order. They do not create
another issue series, process framework or obligation to preserve old experiments.

| Surface | Measured source and active coupling |
| --- | --- |
| Retired frontend installation | [Install-SwarmFrontend](../../tools/modules/Install-SwarmFrontend.ps1) inventories every installed sibling and can rehash retired OpenCode `node_modules` before admitting an unrelated new frontend. Tree/closure helpers: 174 lines; [legacy fixture](../../tools/qualification/Test-InstallFrontendLegacyCompat.ps1): 316. [Install-ForgeWorker](../../tools/modules/Install-ForgeWorker.ps1): 129 lines with no discovered code/CI caller; the used standalone installer already handles Forge. |
| Build provenance | [New-NativeQualification](../../tools/qualification/New-NativeQualification.ps1) rejects commit/manifest/compiler/build-cache-path differences as `HOST_MODULE_BUILD_PROVENANCE_DIVERGED`. [Provenance helpers](../../tools/ci/SwarmBuildProvenanceHelpers.ps1) reconstruct dependencies; builders regex-match private Rust function spelling. [Install-ModuleArtifact](../../tools/modules/Install-ModuleArtifact.ps1) duplicates descriptor validation and copy-provenance records. Actual Antigravity Node resources remain required package files. |
| Retired Python Codex | Four owned Python files under [modules/codex](../../modules/codex/README.md): 4,277 lines plus vendor SDK. [require_new_binding](../../crates/swarm-kernel-host/src/store/task_prompt.rs) rejects its old artifact. Discovered execution callers are fixtures/CI/Justfile; the 165-line Rust [receipt decoder](../../crates/swarm-kernel-host/src/runtime/codex.rs) has Store callers but does not require Python or its SDK. |
| Parallel OpenCode | Host [runtime/opencode_v2](../../crates/swarm-kernel-host/src/runtime/opencode_v2.rs) subtree: 19,948 lines, plus facade/Store code; standalone [adapter](../../crates/swarm-adapter-opencode/src/lib.rs): 24,984 across `src/`. [Legacy demand](../../crates/swarm-kernel-host/src/store/legacy_worker_demand.rs) still activates the built-in route independently of module supervision. Fresh-owned start already rejects; unreachable code after its guards is separate from still-used readback. |
| Parallel Muse | Node `modules/muse/*.mjs`: 4,298 lines; Rust adapter `src/`: 21,774. Node remains the example route; Rust has a disabled template and one recorded exact-result native observation. Neither source size nor that one observation decides which complete implementation to retain. |
| Scheduler/lifecycle duplication | [scheduler.rs](../../crates/swarm-kernel-host/src/scheduler.rs) and [automation_scheduler](../../crates/swarm-kernel-host/src/store/automation_scheduler.rs) both orchestrate due work. Standalone enablement does not disable built-in demand. [legacy_optional_workers](../../crates/swarm-kernel-host/src/host/legacy_optional_workers.rs): 11 worker kinds, 2,337 lines. Duplicate effects are not proven; Store may already suppress them. |
| Document rights and old workflows | [policy.rs](../../crates/swarm-kernel-host/src/policy.rs) requires matching document path/section/SHA before scoped feedback. Separately, [acceptance](../../crates/swarm-kernel-host/src/store/acceptance.rs), [automation acceptance](../../crates/swarm-kernel-host/src/store/automation_acceptance.rs), reviews and Tasks branch on `LegacyV1`. Owner-policy editions and acceptance-policy versions are distinct mechanisms. |
| Mutable automation identity | [automation_transfer::apply](../../crates/swarm-kernel-host/src/store/automation_transfer.rs) creates a successor and disabled old copy, calls six journal relocators plus cron and writes transfer pointers. Its target check makes A→B→A collide with A's retained entry, by source inspection rather than an executed test. |
| Council/interface engines | Concilium: 3,589 lines in three core files, 11 states/eight methods. Integration DTO/Store: 3,747 lines, 14 compatibility dimensions and ACKs. [Integration](../../crates/swarm-kernel-host/src/coordination/integration.rs) is advisory; it does not accept Tasks or change assignments. Wider [coordination](../../crates/swarm-kernel-host/src/store/coordination.rs), 12,697 lines, also contains needed registration and review/submission grants. |
| Automatic publication/labels | [Publication](../../crates/swarm-kernel-host/src/store/automation_publication.rs), [projection](../../crates/swarm-kernel-host/src/store/automation_github_projection.rs) and the automation publication model: 5,371 lines with separate cursors/watermarks and on-behalf contexts. Explicit manual Forge/GitHub publication is a separate retained capability. |

The automation subtree plus Store automation files totals 54,612 physical lines.
Converting all of it to relational storage would retain its product complexity.
Implementation removes unwanted actions before rebuilding storage for the remainder.

## Identity machinery

[LaunchRequest::parse](../../crates/swarm-kernel-host/src/launcher.rs) requires
`plan_digest`; Store [launch_for_actor](../../crates/swarm-kernel-host/src/store/launcher.rs)
recomputes it, and workspace/lease/dispatch consumers compare it again. This is
admitted-plan binding as well as a caller preview requirement. Retain that binding
under the Operation while removing the preview-token roundtrip. Configuration
[apply](../../crates/swarm-kernel-host/src/store/automation.rs) already validates
expected revisions before its additional optional `preview_digest` comparison.

[swarm-store](../../crates/swarm-store/src/lib.rs) computes the SHA of `base_schema`.
`open_writer_inner` checks it before the host initializer; `open_reader` checks it
too. Thus a byte-only SQL edit can block the same database before migrations run.
The host currently fixes `user_version` at 1 and separately installs registered
extensions. This supports removing base-text equality with the existing schema
owner; it does not prove a version number alone validates a malformed database.
See the [bounded replacement](implementation.md#store-and-current-data).

[Participant issuance](../../crates/swarm-kernel-host/src/participant_credentials.rs)
repeats identity/request-slot/profile fingerprints across a manifest and index,
and writes the same index at both opaque-reference paths. `resolve_refs` checks
them against each other. A single authoritative record can remove those copies.
The underlying same-token retry, request/registration binding and credential/profile
verification have real consumers; do not label every digest unused. The
[identity decisions](implementation.md#identity-and-hashes) state what to consolidate.

## Already fixed or unsupported cleanup claims

- Qualification-only automation fixtures are gated. Cached CheckRun acceptance
  loads the source Operation; fair launcher cursor, result-cache and OpenCode
  retention-uncertainty fixes exist. Dead WorkQueue/DemandGate is removed.
- `tasks::require_task_planner` already accepts Operator and Manager. A stale
  operator-only task-planning assignment must not be issued again.
- Shared canonical JSON/digest helpers already exist in `swarm-contracts` and
  `swarm-scripts`. Antigravity already associates a serialized turn with its Operation.
- `swarm-contracts::mcp_catalog::exposes_method` already owns exact exposure.
  Its roughly 230 lines are not a duplicate permission engine. The DTO survey
  did not justify inventing 164 inputs or a schema DSL to adopt Schemars.
- New ScriptRun work rejects queued `executor=None` and requires a selected
  standalone runner. A stale fallback comment does not prove a live default executor.
- GitHub `run_gh_api` is about 104 lines, not a large handwritten HTTP stack.
  octocrab is an optional replacement candidate, not a missing mandatory integration.

## CLI reduction implemented in the preparation branch

[PR #183](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/183), commit
`25a8e3b`, removed duplicate application CLI/MCP launch mapping from
[host main](../../crates/swarm-kernel-host/src/main.rs): 1,949 → 743 physical lines,
net −1,206. Its 87-line parser-test section is unchanged. Public commands remain
in `swarm-cli`; host delegates/internal workers remain. Subsequent documentation
reduction is not additional Rust deletion or completed runtime qualification.
