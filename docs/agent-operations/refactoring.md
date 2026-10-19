# Refactoring — Source Findings

Source rechecked at `9c6192a70382bb12a842f5b960cab904f95908a7` on
2026-10-19. This file records why current mechanisms must be replaced or removed.
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
commits through this revision changed documentation only.

## 1. Common commands still expose adapter internals

`RuntimeCommandKind`, Store policy/capacity and adapters retain vendor/internal
methods:

- `native.opencode.loop_step`;
- Command cancel/close/release-evidence methods;
- `native.mcp.install|observe|arm|read`.

OpenCode loop-step is steering; ACP cancel/close are semantic cancel/shutdown; MCP
phases and evidence cleanup belong inside the adapter. Every leaked method creates
public schemas, policy, capacity, receipts and tests.

Target: closed typed semantic actions. Remove each old method from contracts, Store,
capacity, adapters, descriptors, fixtures and docs in one route slice.

## 2. Package/version metadata became behavior

`module_contract.rs` retains Codex descriptor constructors and Command profile
ladders. `module_handshake.rs` exact-compares artifact ID/version/build, protocol,
capabilities, config schema, pre-input contract and command/event schema arrays, then
adapters compare the echoed set again.

Consequences:

- one optional action creates a new artifact profile;
- Store knows vendor package history;
- adapter startup repeats installation validation;
- metadata behaves like permission.

Target: one current protocol major and a compact capability subset. Package/version
remains installation/inventory data. Store rights remain the authority.

## 3. Adapter bootstrap and delivery are repeatedly reimplemented

Codex, Claude, OpenCode, Muse, Command and Antigravity independently implement env
parsing, owner verification, credentials, protocol/claim parsing, exact capability/
schema matching, hello, command pulling, reconnect and Store-delivery retry.

Existing useful owners already exist:

- `swarm-process::module_owner` for process membership/bootstrap;
- `swarm-client::ModuleLink` for authenticated Eliot IPC.

A common shell is justified only when two current adapters adopt it and delete their
old outer loops in the same change. It must not absorb vendor session state or
recovery.

## 4. Receipts repeat Store-owned identity

Current runtime/outcome/dispatch/result records repeat combinations of:

- module/artifact/version/build/protocol;
- binding/generation;
- Operation and request hashes;
- Task/Attempt/revision;
- Task snapshot/source/prompt hashes;
- nested module receipt;
- selector/result digests.

Store created the Operation, retained the request and authenticated the ModuleLink.
Repeating those coordinates adds comparison code but no trust.

Keep Operation, stale-generation/boot guard, native IDs/disposition and hashes on
actual immutable bytes. Remove package/protocol/request identity from each receipt.

## 5. Vendor proof packets leaked into shared contracts

Shared runtime contains Antigravity session proof and Goal continuation proof types;
OpenCode MCP setup has another public protected envelope.

These are observations or adapter internals:

- model/readiness/stream state → `agent.state`;
- MCP setup → OpenCode adapter;
- Harness Goal → native `Goal`;
- Eliot Goal → ordinary `Send`.

They must not become a second admission protocol.

## 6. Antigravity loses terminal results and couples outcome to observation

The Rust controller retains pending receipts in memory. A terminal result is first
copied into observation state; Store returns an `observation_id`; the adapter rewrites
the pending outcome; Store then compares the outcome with the observation copy.
Existing remediation records show native completion while Store remains
`outcome_unknown` after worker exit.

A durable queue added before removing this dependency would need mutable outcome
revisions and observation dependencies. The correct order is:

1. make terminal outcome self-contained;
2. delete observation/proof coupling;
3. persist the final Store envelope;
4. deliver/retry exact bytes until ACK.

## 7. Execution duplication remains the largest adapter reduction

| Runtime | Current parallel surface | Target |
| --- | --- | --- |
| OpenCode | built-in host subtree plus standalone Rust adapter; legacy demand still activates built-in route | standalone Rust only |
| Muse | Node bridge and Rust adapter | select one by complete behavior and total maintained code |
| Codex | Rust adapter plus retired Python executor/SDK/fixtures | Rust only; keep only required historical result readback |
| Claude | Rust/SDK path plus retained Node generations | one Rust adapter |
| Antigravity | Node warm-stream generations plus Rust | one current warm-stream route |
| Command | Batch v3/v4/v5 and ACP v1/v2 profiles | one current Batch and one current ACP generation |

Historical records do not justify retaining an executor.

## 8. Passive observation is incomplete and partly expensive

Current gaps include:

- binding-only inventory misses unmatched sessions;
- observations require binding;
- family reads filter through Task producers;
- configured source reader has no demand without work;
- process identities are lost before Store projection;
- metric sampling repeatedly hashes executables;
- restart clears stale connection only for `ready`;
- follow events lose source identity;
- remote Git ref observation depends on publication paths.

Target uses adapter-local provider readers, existing process/Git owners and Store
projection. It adds no per-viewer provider poll or transcript mirror.

## 9. WorkDispatch is a duplicate workflow engine

`store/automation_work_dispatch.rs` owns global/per-entry cursors, activation cuts,
catch-up, copied pending Tasks, recheck rotation, semantic slots, duplicate operation
links, quarantine and recent history. `automation/work_dispatch.rs` duplicates launch
settings.

The source has a fixed 128 pending limit. Enough blockers can prevent later eligible
Tasks from being considered. Task revision requires repair of copied state.

Target: ranked current-state queue plus one `dispatch_once` admission. No copied
pending work, source cursor or semantic-slot registry.

## 10. Advisory code scopes duplicate real workspace authority

`code.scope.*` implements proposals, manager acceptance, modes, paths, symbols,
interfaces, digests, revisions, override, expiry, indexes and conflict pages. Its
source explicitly states that it does not lock files, inspect Git or start work.

Workspace launch already stores Task/Attempt/manager/branch/worktree/allowed paths
and rejects overlapping active leases before filesystem work. That is the real write
authority.

Target: repository-relative Task path prefixes become the Attempt lease. Delete
`code.scope.*` after current callers switch.

## 11. Process ownership repeats compatibility coordinates

`ModuleOwnerPlan` and `ProtectedResolverMap` repeat binding/generation, artifact
ID/version/build, protocol and full module contract, then compare them again during
credential resolution. These coordinates already exist in installation and Store
binding records.

Keep exact process birth, executable, group/Job ownership and private credential
reference. Remove runtime package-contract negotiation from the process owner.

## 12. Capacity is a second Operation lifecycle database

`store/capacity.rs` materializes every native-related Operation/producer into
reserved/active/released phases with execution identities, native event refs, unknown
timestamps and per-scope `meta` rows. It includes readback/control methods such as
`refresh`, `result` and `reconcile` in capacity accounting.

It supports whole-blob v1 and per-entry v2 layouts, encoded keys, migration,
refusal records, damage handling and lifecycle sync from many Store mutations.

The product currently needs one configured root limit. Root claims are already
derivable from unresolved root-opening Operations and unreleased bindings. A second
ledger creates drift and code without adding authority.

Target: count and reserve root ownership in the launch/open transaction; report
provider and host facts separately; delete the ledger and its Store hooks.

## 13. Provider conditions are a Codex-specific proof matrix

`NativeUsageSnapshot` knows only Codex/Muse services and exact native method names.
It contains Codex-specific ordinary-usage permission, auth-context SHA, evidence
methods/revisions and many provider-specific fields.

`ProviderConditionFact` repeats route, binding, generation, native scope, provider,
account, model, bucket, source method/revision/connection, auth SHA and sequence.
Store persists roughly the same matrix in relational columns plus a digest.

Behavioral problems:

- cross-binding reuse depends on a productive source binding staying current;
- a stale source can hold a route indefinitely;
- rate-limit/quota facts remain blocking after retry/reset until a newer exact fact;
- Store rejects Muse Rust usage through artifact-source allowlists;
- account/service facts are copied per binding.

Target: source-scoped usage and availability records with honest scope, freshness and
coverage. Native method names stay in adapters. Transient retry/reset opens one probe;
hard negatives require source/config change or explicit fallback.

## 14. Three scheduler/control systems overlap

Current source contains:

1. `scheduler.rs`: in-process timer for static CheckRun schedules, manager cron and
   Goals;
2. `store/automation_scheduler.rs` + `swarm-automation-worker`: a second process
   paging/digesting/admitting the same due sources through its own credential,
   generation, owner and process group;
3. `supervise_automation`: another two-second automation/watch loop.

The eleven-slot legacy coordinator additionally owns Scheduler, AutomationScheduler,
Automation, Launcher, NativeMcp, NativeMcpTools and other workers, each with demand,
backoff, shutdown and status machinery.

A timer does not need a service identity, credential, process custody protocol,
source digests and RPC. Durable due state already belongs to Store.

Target: one in-process admission loop reacts to Store changes and nearest due time,
with a slow recovery wake. External-effect workers remain separate only when they
perform real external I/O/custody.

## 15. Automation identity and product breadth remain excessive

Automation transfer still clones entries, moves several journals and follows
successor relations. Owner/name is incorrectly part of identity.

Retained optional product engines include:

- Concilium/council;
- generic integration ratification/ACK;
- automatic publication/Issue-label projection;
- document-edition permission behavior;
- overlapping schedulers and legacy optional workers.

Target: immutable `entry_id`, mutable owner/revision, typed retained helpers only.
Delete optional product engines outside compact-core completion.

<a id="code-reduction-candidates"></a>

## 16. Additional confirmed reduction candidates

- frontend installation inspects retired sibling trees and keeps duplicate Forge/
  legacy installers;
- qualification treats checkout/compiler/cache-path provenance as compatibility;
- host links worker implementation crates;
- most pure reads still pass through the writer;
- base SQL text SHA can reject a semantically compatible DB before migration;
- Muse Rust stop can wait indefinitely;
- direct acceptance and automatic acceptance do not share the same candidate-specific
  review prerequisite;
- launch preview hashes and MCP readiness delay direct work;
- current route validation hardcodes artifact IDs and vendor option shapes.

These remain in the single implementation order; do not create another issue series.

## 17. Claims explicitly rejected

- MCP exposure policy is not automatically duplicate permission code.
- Schemars is not a proven net deletion for current hand-built MCP inputs.
- Octocrab is optional; current `gh` transport is not a giant HTTP implementation.
- Result-page retention before ACK is necessary.
- Local transcript, alive PID or mtime does not settle uncertain native write.
- File moves, a Rust rewrite or more crates do not simplify retained responsibility.
- A new Lane entity is not required; current route/automation/queue facts already
  provide the view.
- CCCC suspended launch is not needed while owner-first process topology has no
  child-before-Job race.

## Implemented source reduction

Commit `25a8e3b` removed the host's duplicate application CLI and MCP launcher:
`main.rs` fell from 1,949 to 743 physical lines, net −1,206 Rust lines. Public
commands remain in `swarm-cli`; daemon/admin/internal-worker entrypoints remain.
Later documentation changes are not runtime deletion.
