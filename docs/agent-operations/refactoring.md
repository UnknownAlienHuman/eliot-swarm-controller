# Refactoring — Source Findings

Source rechecked at `4cfbbf84dfde7efc60c5dd6e17f1f5bc48377800` on
2026-10-18. This file records why current mechanisms must be replaced or removed.
[Architecture](architecture.md) owns product law;
[implementation](implementation.md) owns the only work order;
[status](../implementation-status.md) records completed work. Existing code is not a
requirement to preserve every subsystem.

## Size and concentration

| Owned Rust scope | Physical lines after CLI deletion | Explicit test-only estimate |
| --- | ---: | ---: |
| All owned Rust, excluding `vendor/` | 533,720 | 172,511 |
| `crates/swarm-kernel-host` | 365,726 | 128,996 |

Counts include blanks/comments. They describe concentration, not reachable production
SLOC. Commit `25a8e3b` removed 1,206 net Rust lines from duplicate host CLI. Later
commits through this source revision changed documentation only.

<a id="code-reduction-candidates"></a>

## 1. WorkDispatch is a second workflow engine

`store/automation_work_dispatch.rs` does not merely select a Task. It owns:

- a global cursor over enabled automation entries;
- per-entry Task-observation cursor and catch-up cut;
- copied pending subjects with wake reasons and timestamps;
- bounded recheck rotation;
- semantic launch slots and operation-link records;
- source/config quarantine records;
- recent projections and multiple recovery paths.

`automation/work_dispatch.rs` separately reconstructs route, profile, workspace,
model, effort, budgets, stop conditions and preview requests.

This creates the failure reported by the old swarm audit: once the copied pending list
reaches its bound, later eligible work can remain behind unrelated blockers. A Task
revision must also repair a copied pending record that should never have existed.

Replacement: current-state `swarm.queue.get` plus one `dispatch_once` admission
transaction. Blocked Tasks remain Tasks; scheduler wakes query them again. Delete
cursors, pending copies, semantic slots, ordinary-blocker quarantine and preview hash
binding.

## 2. Lane is missing operationally but should not become another entity

The old scripts used a lane as a useful bundle of:

- route/provider/model/effort policy;
- queue ordering;
- capacity and quota behavior;
- one manager/worktree ownership rule;
- stop-new-work/drain behavior.

Current source already has the durable inputs:

- manager/project automation entry;
- `Route` and `RouteAdmissionPolicy.max_concurrent_roots`;
- provider conditions and capacity;
- Tasks/Attempts;
- bindings and workspace leases.

A new lane table/process/cursor/inbox would duplicate all of them. The target lane is
a named projection/configuration over existing facts, using the automation entry as
policy identity. UI and reports can display it without Core inventing another state
machine.

## 3. Advisory code scopes duplicate the actual workspace lease

`coordination/code_scope.rs` and `store/code_scopes.rs` implement:

- propose/accept/release;
- exclusive/shared/read modes;
- paths, symbols and interfaces;
- proposal SHA, expected revision and broad-scope acknowledgement;
- overrides and expiry;
- task pages, active indexes and conservative conflict coverage;
- scope revisions consumed by coordination/contract-decision code.

The source explicitly says these records do not lock files, inspect Git, start work or
release an Attempt.

Actual launch already stores `allowed_paths`/`allowed_symbols` in
`workspace_leases` and calls `reject_scope_conflicts` before reservation. TaskSpec
already has `scope.initial_paths` and `forbidden_paths`.

Replacement: the workspace lease is the only write authority. Use repository-relative
path prefixes and Task/Attempt lifecycle. Delete symbols/interfaces/globs as
concurrency authority and remove the entire `code.scope.*` API after callers migrate.

## 4. Workspace authority is real but over-proven

`store/workspace.rs` correctly separates bounded Store reservation from external
filesystem/Git work and retains unknown outcomes. It also carries extensive repeated
digests and readback proofs across registration, lease, plan, branch, worktree and
revocation paths.

Do not remove the durable lease or unknown-effect readback. Simplify after the scope
migration:

- one current workspace registration generation;
- one Attempt-bound lease;
- normalized write prefixes and forbidden prefixes;
- exact operation/owner/generation;
- actual baseline, branch, path and clean-state readback;
- no advisory scope revision, symbol set, proposal digest or document edition.

A hash can protect selected bytes. It should not substitute for direct equality of
current Store IDs and filesystem/Git facts.

## 5. The common command bus still leaks vendor/internal methods

Current `RuntimeCommandKind` and Store policy include:

- `native.opencode.loop_step`;
- `native.command.cancel_turn` and `native.command.close_session`;
- `native.command.release_evidence`;
- `native.mcp.install|observe|arm|read`.

These names propagate into capacity, method policy, descriptors, adapters and tests.
They are not distinct product actions:

- loop step is `Send` with steer semantics;
- turn cancel is `Cancel`;
- close is `Shutdown`;
- release evidence and MCP phases are adapter lifecycle details.

Replacement: typed semantic actions in implementation. Delete each old method from
all owners in the same route migration.

## 6. Package versions became behavioral compatibility

`module_contract.rs` retains Codex and Command profile/version ladders.
`module_handshake.rs` exact-compares artifact ID/version/build, protocol,
capabilities, config schema, command schemas and event schemas, then adapters compare
the echoed set again. Store also contains vendor artifact/version checks.

Consequences:

- each optional action creates another artifact profile;
- Core knows vendor package history;
- adapters preserve old schema combinations;
- installation metadata becomes runtime authorization.

Replacement: one current protocol major and compact capability subset. Package/version
remains installation/inventory data. Store rights remain the authorization boundary.

## 7. Every adapter repeats bootstrap and Store delivery

Codex, Claude, OpenCode, Muse, Command and Antigravity repeat variants of:

- environment/owner/credential parsing;
- protocol and full descriptor claim parsing;
- exact capability/schema equality;
- hello response validation;
- `module.next` pulling;
- outcome/observation/result re-delivery;
- Eliot IPC reconnect.

Useful current owners already exist:

- `swarm-process::module_owner` for process membership/private state;
- `swarm-client::ModuleLink` for authenticated IPC/hello.

Extract only after two adapters delete their previous code in the same slice. Do not
create a framework crate or generalize the current huge journals.

## 8. Current adapter journals combine unrelated responsibilities

Approximate main-file sizes at the reviewed revision:

| Adapter | Main journal file |
| --- | ---: |
| Command | ~200 KB |
| Claude | ~183 KB |
| OpenCode | ~139 KB |

They combine native intent, effect recovery, Store delivery, outcome revisions,
result selectors, references, startup scans, torn-tail handling, checkpoints,
tombstones, cleanup queues, retention cursors and compaction.

This is not a donor for a shared journal. Keep native effect state in each adapter.
Share only exact delivery of already-produced Store envelopes after two routes prove
the same immutable-file semantics.

## 9. Receipts repeat identity already owned by Store

Current command/outcome/dispatch/result structures repeat combinations of:

- module/artifact/version/build/protocol;
- binding/generation and Operation;
- request/input SHA;
- Task/Attempt/revision;
- Task snapshot and source hashes;
- payload/selector/result hashes;
- full module receipt inside later receipts.

Store created the Operation and authenticated the ModuleLink. The adapter cannot
legitimately select another request. Keep Operation, boot/generation/native IDs,
disposition and hashes/ranges for actual immutable result bytes. Remove package and
request proof packets.

## 10. Antigravity couples terminal truth to observation

Antigravity terminal result is copied into observation, assigned a Store
`observation_id`, then referenced from a rewritten outcome. Store validates the
outcome by re-reading the same result copy from the current observation. Dispatch also
requires an `AntigravityDispatchSessionProof` generated by refresh.

This creates a mutable dependency chain before terminal delivery and turns diagnostics
into authorization.

Replacement order:

1. make terminal outcome self-contained;
2. remove observation result copy, observation ID and dispatch proof;
3. add a tiny immutable delivery spool;
4. migrate real semantic actions and receipts.

## 11. Quota/provider state is another proof matrix

`NativeUsageSnapshot` currently knows Codex/Muse services, exact native method names,
provider-specific fields and an `auth_context_ref` SHA. `ProviderConditionFact`
repeats route, binding, generation, provider/account/model/bucket, connection,
method/revision and sequence. Store persists a wide duplicate row plus a details
digest. Muse Rust is rejected because Store accepts only specific Node bridge
versions.

Replacement: generic source identity/epoch/sequence/freshness/coverage, actual scope,
availability condition, retry/reset and native-labelled buckets. Adapter owns native
method interpretation. Store validates authenticated source and monotonic order, not
artifact version.

## 12. Process ownership is secure but coordinate-heavy

`ModuleOwnerPlan` and `ProtectedResolverMap` repeat module, binding, generation,
artifact ID/version/build, protocol and full module contract. The resolver compares
the whole matrix again before supplying protected file references.

Keep:

- exact owner-first Job/process group;
- binding/generation/boot/client identity;
- private state and credential reference;
- path containment/no-link traversal/private permissions;
- executable/package verification at install/launch;
- PID plus strong OS process-birth identity.

Remove repeated runtime compatibility comparison from the protected-ref resolver.
Restart reconciliation kills only positively matched Eliot-owned leftovers. PID
mismatch clears the record without killing; uninspectable remains unknown.

CCCC's suspended-child primitive is unnecessary for the current owner-first topology.

## 13. Stale launcher documentation still describes a second product

`docs/swarm-launcher-assignment-context.md` calls itself a normative amendment and
specifies a large dashboard, queue, assignment preview, mandatory launch preview,
capability receipt, watches, consultation, contract publication and integration
surfaces.

Useful requirements are smaller:

- compact TaskPrompt;
- deterministic explainable queue projection;
- direct launch revalidation;
- workspace/write lease;
- route/provider/capacity facts;
- local observation and explicit uncertainty.

The file must become a concise guide subordinate to architecture/implementation, not
another product authority.

## 14. Execution duplication remains the largest deletion opportunity

| Runtime | Parallel surface | Target |
| --- | --- | --- |
| Antigravity | Node warm-stream generations + Rust adapter | Rust route after A0–A3; remove Node package/path |
| OpenCode | built-in host subtree + standalone Rust adapter | standalone Rust only; remove host executor/legacy demand/native MCP public phases |
| Muse | Node bridge + Rust adapter | select one by complete behavior and total Rust+JS+glue |
| Codex | Rust adapter + retired Python executor/SDK/fixtures | Rust only; keep only small historical readers actually used |
| Claude | Rust/SDK path + retained Node generations | one Rust adapter |
| Command | Batch v3/v4/v5 + ACP v1/v2 profiles | one current batch and/or one current ACP product requirement; remove profile ladders |

Historical completed records do not keep executors alive. Concrete unsettled work gets
a finite disposition.

## 15. Product engines outside the compact core

The following remain source-deletion candidates:

- Concilium/council state machine;
- integration negotiation/ratification/ACK engine;
- automatic publication/Issue-label projection;
- document-edition permissions and legacy acceptance branches;
- duplicate installers/provenance equality/qualification frameworks;
- host dependencies on worker implementation crates;
- parallel schedulers and automation transfer relocators;
- owner-dependent automation identity and `meta` key codecs.

Do not first finish or generalize these systems. Remove their product requirement,
then delete the code and callers.

## 16. Claims explicitly rejected

- Exact MCP exposure already has one owner; do not remove it as “duplicate policy.”
- Schemars is not a ready deletion where no shared input DTO exists.
- Octocrab is optional; current `gh` transport is not a major handwritten HTTP stack.
- Result-page retention before ACK is required recovery, not waste.
- A local transcript, live PID or recent mtime does not settle an uncertain native
  write or prove Task completion.
- A lane table, code-scope proposal engine or shared adapter journal would not make
  the product simpler.
- File moves, language rewrites and more crates are not deletion.

## Implemented source reduction

Commit `25a8e3b`, now in `main`, removed the host's duplicate application CLI and MCP
launcher: `main.rs` fell from 1,949 to 743 physical lines, net −1,206 Rust lines.
Public commands remain in `swarm-cli`; host daemon/admin/internal-worker entrypoints
remain. Later documentation changes are not runtime deletion.
