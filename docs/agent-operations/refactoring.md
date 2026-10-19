# Refactoring — Source Findings

Runtime source rechecked through `e6b03b72ccec03b378c70b883f51ba2f34fde8d2`;
audit/status documentation was updated through
`aeb9c2a4c7f00d609cd7b1e3c176362088d42371` on 2026-10-19.
[Architecture](architecture.md) owns the product boundary;
[implementation](implementation.md) owns the only work order;
[status](../implementation-status.md) records completed source changes.

Source presence is not a requirement to preserve a subsystem.

## Size and concentration

| Owned Rust scope | Physical lines after CLI deletion | Explicit test-only estimate |
| --- | ---: | ---: |
| All owned Rust, excluding `vendor/` | 533,720 | 172,511 |
| `crates/swarm-kernel-host` | 365,726 | 128,996 |

Counts include blanks/comments. They show concentration, not reachable production
SLOC. Commit `25a8e3b` removed 1,206 net Rust lines from duplicate host CLI. Later
commits through the source revision above changed documentation only.

## 1. Common commands expose adapter internals

`RuntimeCommandKind`, Store policy/capacity and adapters retain:

- `native.opencode.loop_step`;
- Command cancel/close/release-evidence methods;
- `native.mcp.install|observe|arm|read`.

OpenCode loop-step is steering; ACP cancel/close are semantic cancel/shutdown; MCP
phases/evidence cleanup belong inside the adapter. Each leaked method creates schemas,
policy, receipts, capacity hooks and tests.

Target: closed typed semantic actions. Remove each old name from contracts, Store,
adapters, descriptors, fixtures and docs in one route slice.

## 2. Package metadata became behavior

`module_contract.rs` retains Codex descriptor constructors and Command profile
ladders. `module_handshake.rs` exact-compares artifact ID/version/build, protocol,
capabilities, config schema, pre-input contract and command/event schema arrays;
adapters compare the echo again.

One optional action therefore creates a package profile, Store branch, adapter startup
branch and compatibility fixture. Package metadata behaves like permission.

Target: one current protocol major and capability subset. Package/version remains
install/inventory data; Store rights remain authority.

## 3. Adapter bootstrap/delivery is reimplemented

Codex, Claude, OpenCode, Muse, Command and Antigravity independently parse env,
verify owner scope, load credentials, negotiate claims, hello, pull commands,
reconnect and redeliver Store records.

Existing owners already cover the common boundary:

- `swarm-process::module_owner` — process membership/bootstrap;
- `swarm-client::ModuleLink` — authenticated Eliot IPC.

Extract only after two current adapters adopt the same small shell and delete both old
loops. Native session state and recovery remain adapter-owned.

## 4. Receipts repeat Store-owned identity

Runtime/outcome/dispatch/result records repeat combinations of module/package/
protocol, binding/generation, Operation/request hashes, Task/Attempt/revision,
Task/source/prompt hashes, nested receipt and selector/result digests.

Store created the Operation, retained the request and authenticated ModuleLink.
Repeating those coordinates adds comparisons but no trust.

Keep Operation, stale generation/boot guard, observed native IDs/disposition and
hashes on actual immutable bytes. Remove package/protocol/request and duplicated
Task/source identity from receipts.

## 5. Vendor proof packets leaked into shared contracts

Shared runtime includes Antigravity session proof, Goal continuation proofs and
OpenCode MCP protected envelopes.

These are observations or adapter internals:

- model/readiness/stream → `agent.state`;
- MCP setup → OpenCode adapter;
- Harness Goal → native `Goal`;
- Eliot Goal → ordinary `Send`.

They must not become a second admission protocol.

## 6. Antigravity loses terminal delivery

Pending receipts are in memory. A terminal result is first copied into observation;
Store returns `observation_id`; the adapter rewrites the outcome; Store compares it
with the observation copy. Native completion can therefore survive while Store stays
`outcome_unknown` after worker exit.

A durable queue before removing this dependency would need mutable records and
observation edges. Correct order:

1. self-contained terminal outcome;
2. delete observation/proof coupling;
3. persist final Store envelope;
4. retry identical delivery until ACK.

## 7. Parallel executors remain the largest adapter deletion

| Runtime | Current parallel surface | Target |
| --- | --- | --- |
| OpenCode | built-in host subtree plus standalone Rust; legacy demand still activates built-in path | standalone Rust only |
| Muse | Node bridge and Rust adapter | select one by complete behavior and total maintained code |
| Codex | Rust adapter plus retired Python executor/SDK/fixtures | Rust only; retain required historical result readback only |
| Claude | Rust/SDK path plus retained Node generations | one Rust adapter |
| Antigravity | Node warm-stream generations plus Rust | one current warm-stream route |
| Command | Batch v3/v4/v5 and ACP v1/v2 profiles | one current Batch and one current ACP generation |

Historical records do not justify an executable.

## 8. Passive observation is incomplete and expensive

Current gaps include binding-only inventory, binding-required observations, family
filtering through Task producers, no configured source demand without work, lost
process identities, per-sample executable hashing, incomplete restart disconnect,
source-less follow events and publication-dependent remote-ref reads.

Target uses adapter-local provider readers, existing process/Git owners and Store
projection. No per-viewer provider poll or transcript mirror.

## 9. WorkDispatch is another workflow engine

`automation_work_dispatch.rs` owns global/per-entry cursors, activation cuts, copied
blocked Tasks, recheck rotation, semantic slots, links, quarantine and recent
history. `automation/work_dispatch.rs` duplicates launch settings. A fixed 128 pending
limit can hide later eligible Tasks.

Target: ranked current-state queue plus one `dispatch_once`. No copied pending work,
source cursor or launch-slot registry.

## 10. Advisory code scopes duplicate workspace authority

`code.scope.*` implements proposals, manager acceptance, modes, paths, symbols,
interfaces, digests, revisions, override, expiry, indexes and conflict pages, while
its own source says it does not lock files, inspect Git or start work.

Workspace launch already retains Task/Attempt/manager/branch/worktree/paths and rejects
overlapping active leases before filesystem work. That is the real authority.

Target: Task path prefixes become Attempt lease; delete `code.scope.*`.

## 11. Process ownership repeats compatibility coordinates

`ModuleOwnerPlan` and `ProtectedResolverMap` repeat binding/generation, package
ID/version/build, protocol and full module contract, then compare them again during
credential resolution.

Keep process birth, executable, group/Job ownership and private credential reference.
Remove runtime package-contract negotiation from the process owner.

## 12. Capacity is a second Operation lifecycle database

`capacity.rs` materializes native-related Operations/producers into reserved/active/
released phases with execution identities, event refs, unknown timestamps and `meta`
rows. It counts read/control methods such as refresh/result/reconcile.

It supports whole-blob v1 and per-entry v2 layouts, encoded keys, migration, refusal
records, damage handling and lifecycle sync hooks.

The retained product needs a route root limit. Claims are derivable from unresolved
root-opening Operations and unreleased bindings. Target: count/reserve in the launch/
open transaction and delete the ledger.

## 13. Provider conditions are a vendor proof matrix

`NativeUsageSnapshot` knows Codex/Muse services and exact native method names. It
carries Codex-specific permission, auth SHA and evidence method/revision.
`ProviderConditionFact` repeats route, binding, generation, native scope, provider,
account, model, bucket, connection, auth SHA and sequence.

Consequences:

- productive binding identity incorrectly anchors account/service facts;
- stale source can hold a route;
- transient conditions remain blocking after retry/reset;
- Muse Rust is rejected by artifact allowlist;
- account facts are copied per binding.

Target: source-scoped usage/availability with honest scope, freshness and coverage;
native names remain in adapters.

## 14. Three scheduler/control systems overlap

Current source has:

1. in-process `scheduler.rs` for checks, cron and Goals;
2. `automation_scheduler.rs` plus standalone `swarm-automation-worker` paging and
   admitting the same sources through its own credential/process lifecycle;
3. another short-interval automation/watch loop.

The legacy coordinator also owns Scheduler, AutomationScheduler, Automation,
Launcher, NativeMcp, NativeMcpTools and other worker slots.

A timer does not need service identity, credential, process group, source digest and
RPC. Target: one in-process admission loop over Store due/current state.

## 15. TaskPrompt sends controller internals to the model

`TaskPromptEnvelopeV1` repeats Task/Attempt/revision, snapshot hash, prompt hash/size
and text. `TaskDispatchContext`, admission/result records and adapters repeat source
text hashes/lengths and the same Task identity. `store/task_prompt.rs` also injects
canonical identity JSON, canonical Task brief and launch packet into the model text.

The model needs current objective, requirements, selected sources, write boundary,
workspace, deliverable and checks—not controller proofs. Internal changes currently
alter prompt bytes and force adapter compatibility work even when the assignment is
unchanged.

Target: one bounded `TaskPacket` renderer for initial, correction and resume. Store
identity remains in Operation/Store; remove repeated hashes/JSON from model text and
adapter receipts.

## 16. Review/correction is another workflow family

One review can pass through ReviewDisposition, `task.request_changes`, findings
package, RepairDispatch, semantic slot, operation link, `agent.send` and a separate
Acceptance consumer. Each repeats Task/Attempt/candidate/review/manager identity.

`review.submit` already retains exact candidate verdict/findings;
`task.request_changes` is the real Task transition.

Target:

- `review_once` assigns missing review;
- `task.request_changes` stores complete findings and optionally sends one correction;
- `accept_once` calls the common acceptance gate;
- candidate-specific bounded correction rounds stop no-progress.

Delete disposition/repair workflow state, cursors, slots, links and duplicate evidence
packages.

## 17. Public API is maintained in parallel registries

Current method behavior is split across at least:

- `swarm-contracts::method_policy::METHOD_REGISTRY`;
- MCP catalog/profile/schema exposure;
- `model::validate_mutation` field validation;
- `Store::call` role/prefix/special routing;
- `Store::apply` mutation routing;
- participant special-case dispatch and `mcp_authorization`;
- `OPERATION_VISIBILITY_SQL` method-name branches.

The same file families still expose `code.scope`, Concilium, contract/ACK,
publication/GitHub projection, vendor-native commands and standalone scheduler RPC.
No privilege bypass was proven in this pass; domain handlers often recheck exact
scope. The confirmed defect is structural: removal/addition requires editing many
independent maps, and authorization is partly coarse method classification and partly
manual object logic.

Target: separate public, internal-host and adapter protocols. Retained domains own one
typed parser/authorizer/handler. Normalize Operation scope and remove method-name
visibility SQL as old products disappear. Do not replace this with a generic policy
DSL.

## 18. Store migration verifies source text instead of schema version

`swarm-store::SchemaIdentity` hashes exact `001_core.sql`, writes
`meta.schema_digest`, keeps `user_version=1`, and refuses writer/read-only open when
the current binary's SQL text differs. Generic extensions digest-pin their SQL.

Three newer migrations have special Rust installers:

- `script_event_schema.rs` builds an in-memory reference DB on every open, compares
  schema objects/columns/foreign keys/indexes and contains a custom SQL tokenizer;
- failure and cancellation event migrations duplicate trigger-reference DB and exact
  trigger SQL verification modules;
- each migration has its own markers and fixtures beside generic extension handling.

This protects exact historical source text, not merely data compatibility. Equivalent
DDL or a legitimate edited base migration can brick open before an explicit migration
runs. No current data-loss bug was proven; the confirmed cost is a second schema
compiler/compatibility system and blocked evolution.

Target: application ID + monotonic numeric migration version in one IMMEDIATE
transaction. Bridge concrete existing marker states once; then delete base SQL digest,
custom lexer/reference DB and per-migration verifier modules. Use explicit migration
pre/postconditions and optional SQLite integrity checks, not exact DDL equality.

## 19. Automation identity and optional product breadth remain excessive

Automation transfer clones entries, moves journals and follows successor relations;
owner/name is incorrectly part of identity.

Optional engines still include:

- Concilium/council;
- generic contract/integration ratification and ACK;
- automatic publication/Issue-label projection;
- document-edition permission behavior;
- overlapping schedulers/legacy workers.

Target: immutable `entry_id`, mutable owner/revision, typed retained helpers only.
Delete engines outside compact-core completion.

<a id="code-reduction-candidates"></a>

## 20. Additional reduction candidates

- frontend installation inspects retired sibling trees and duplicate installers;
- qualification treats checkout/compiler/cache path as compatibility;
- host links worker implementation crates;
- most pure reads still pass through the writer;
- Muse Rust stop can wait indefinitely;
- launch preview hashes and MCP readiness delay direct work;
- current route validation hardcodes package IDs and vendor option shapes.

These remain in the single implementation order; do not create another issue series.

## 21. Claims explicitly rejected

- MCP exposure policy is not automatically duplicate permission code. The problem is
  parallel routing/authorization/visibility ownership, not the existence of one tool
  catalog.
- Schemars is not a proven net deletion for current MCP inputs.
- Octocrab is optional; current `gh` transport is not a giant HTTP implementation.
- Result-page retention before ACK is necessary.
- Local transcript, alive PID or mtime does not settle uncertain native writes.
- File moves, a Rust rewrite or more crates do not simplify retained responsibility.
- A Lane entity is unnecessary; current policy/route/queue facts provide the view.
- CCCC suspended launch is unnecessary while owner-first topology has no proven race.
- Numeric migrations do not require an ORM or migration framework; ordered SQL and
  current SQLite/rusqlite transaction owners are sufficient.

## Implemented source reduction

Commit `25a8e3b` removed the host's duplicate application CLI and MCP launcher:
`main.rs` fell from 1,949 to 743 physical lines, net −1,206 Rust lines. Public
commands remain in `swarm-cli`; daemon/admin/internal-worker entrypoints remain.
Later documentation changes are not runtime deletion.
