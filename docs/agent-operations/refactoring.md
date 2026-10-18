# Refactoring — Source Findings

Source was rechecked through
`513f9698c268753dc6f2a6c0cf4d9a53f7f5e472` on 2026-10-18.
[Architecture](architecture.md) owns the product boundary;
[implementation](implementation.md) owns the only work order;
[status](../implementation-status.md) records completed changes. This file records
why current mechanisms must be removed or replaced. Source presence is not a
requirement to preserve a subsystem.

## Size and concentration

| Owned Rust scope | Physical lines after CLI deletion | Explicit test-only estimate |
| --- | ---: | ---: |
| All owned Rust, excluding `vendor/` | 533,720 | 172,511 |
| `crates/swarm-kernel-host` | 365,726 | 128,996 |

Counts include blanks/comments and do not equal reachable production SLOC. Commit
`25a8e3b` removed 1,206 net Rust lines from the duplicate host CLI. Later commits
through this source revision changed documentation or created/removed a no-op edit
workflow; the runtime tree remained unchanged.

## 1. Why agents could not finish the project

The main failure was horizontal construction: contracts, schemas, proof packets,
qualification modes and compatibility branches multiplied while a manager still
could not rely on one short assignment-to-result path.

The old Claude swarm confirms the pattern. Its shadow controller contained large
state/queue scripts, lane launchers, monitors, reminder/restart daemons and multiple
acceptance pipelines. Compact `TASK.md` microtasks completed; whole Issue threads and
addendum chains produced meta-work and conflicting interpretations. The useful lesson
is one current slice packet and one manager-owned worktree, not porting the scripts.

The development loop must reward:

- one observable vertical behavior;
- exact source symbols;
- explicit failure cases;
- deletion of the replaced route;
- one independent verification after integration;
- net owned-code reduction.

## 2. Antigravity can lose a completed outcome

`modules/antigravity-rust/src/controller.rs::pending_receipts` is a bounded in-memory
queue. `main.rs::flush_reports` removes an entry only after Store ACK, but the bytes
are not durable before that ACK.

The repository's own Antigravity S2 receipts record the resulting gap: Store remained
`outcome_unknown`, and after worker exit direct reconcile depended on the dead
process's in-memory receipts and otherwise returned `NATIVE_HISTORY_UNAVAILABLE`.
The controller comments also state that its pending receipts and completed-turn cache
are same-process only.

This is not a reason to add a universal event store. The missing responsibility is a
small route-local Store-delivery outbox containing only already-produced outcome,
observation and result envelopes. Native prompts remain owned by the native effect
path and are never replayed because Store delivery failed.

## 3. Active Antigravity coordinates disagree

The current Rust source and shipped descriptor use artifact version `6`:

- `modules/antigravity-rust/src/wire.rs::ARTIFACT_VERSION`;
- `modules/antigravity-rust/module-descriptor.template.json`.

Active tooling still selects or validates version `5`:

- `modules/antigravity-rust/route.example.toml` comment;
- `tools/ci/module-package-policy.json`;
- `tools/qualification/New-NativeQualification.ps1::Get-AdapterContract`;
- `tools/modules/Install-ModuleArtifact.ps1::Assert-AntigravityV5DescriptorContract`.

Store accepts both `5 | 6`, which turns a packaging drift into a permanent runtime
branch. Historical v5 receipts are valid history; new-work selectors must converge on
v6, then the active dual-version branch must disappear after finite disposition of
concrete v5 bindings.

## 4. Antigravity readiness became a second authorization system

`AntigravityDispatchSessionProof` contains:

- binding/generation/worker boot;
- native root and a SHA of the native scope;
- requested and observed model;
- stream phase;
- event/result/gap counters;
- pending state and stream errors/exits.

The adapter emits it from `agent.refresh`. Store then loads the latest refresh,
revalidates the proof against the binding and observation, compares the stream
snapshot again, hashes the proof, hashes assignment and lease authority, and embeds
all digests in the launch packet.

This machinery does not establish a new right. Store already owns assignment,
binding, generation, workspace lease and Operation. The adapter already owns its
boot and native session. Model and stream facts are observations.

Target dispatch preconditions are current assignment, ready unreleased binding,
current adapter boot/session, one free serialized input window and expected revisions.
Delete the proof DTO, producer, Store validator, packet digests and refresh gate.
Keep the facts in `agent.state`.

## 5. Antigravity carries unsupported actions and duplicate identity

The descriptor advertises six capabilities, but `main.rs` and `wire.rs` still accept
or reject placeholder branches for reply, configure, Goal, background and recover.
Unsupported features therefore occupy method parsers, tests and adapter code despite
never being routable.

`OperationIdentity` and `ModuleReceiptIdentity` then repeat:

- package/artifact/version/protocol;
- binding/generation;
- Operation ID;
- request/input hash;
- target hash for readback;
- Task/Attempt/source identity again in dispatch admission.

Store created and retained the Operation and authenticated the binding-scoped link.
Repeating the package and request identity in every outcome adds comparison code, not
trust. Keep only stale-report identity, native IDs/disposition and hashes of actual
retained bytes.

## 6. The shared adapter bus is still vendor-shaped

`RuntimeCommandKind` contains semantic actions beside internal/vendor methods:

- `native.opencode.loop_step`;
- `native.command.cancel_turn` / `close_session` / `release_evidence`;
- `native.mcp.install|observe|arm|read`.

Store policy, capacity, descriptors, adapters and tests repeat these names. OpenCode
loop-step is steering; ACP cancel/close are cancel/shutdown; MCP phases and release
evidence are internal lifecycle details.

The target typed actions are documented in implementation. Each old method must be
removed from contracts, Store, capacity, descriptors, adapters, fixtures and docs in
the same route slice.

## 7. Package versions became behavior profiles

`module_contract.rs` and adapter code retain Codex version ladders, Command Batch
v3/v4/v5, ACP v1/v2 and exact schema/capability combinations. Store hello then
compares module ID, artifact version/build, protocol, config schema, command/event
schemas and capabilities, and adapters compare the echoed values again.

Consequences:

- adding one optional action mints another artifact profile;
- Core knows vendor package history;
- old schema combinations become permanent code paths;
- descriptor metadata acts like authorization despite Store rights being separate.

Use one current protocol major plus semantic action subset. Package identity remains
installation/inventory data and may aid rollback; it does not select Core behavior.

## 8. Every adapter repeats bootstrap and Eliot transport

Codex, Claude, OpenCode, Muse, Command and Antigravity independently parse supervisor
environment, owner membership, state/credential paths, binding identity, protocol and
full descriptor claims. They also repeat hello validation, `module.next`, reconnect
and Store delivery loops.

Existing owners already cover the reusable boundary:

- `swarm-process::module_owner` verifies the supervisor-owned process;
- `swarm-client::ModuleLink` owns authenticated hello and module RPC calls;
- `swarm-process::permissions` provides atomic private durable files.

The first bootstrap extraction candidates are Antigravity and Claude's contained
`OwnedBootstrap`. Command is explicitly a bad proof because its historical
Batch/ACP matrix and ACP state machine would leak into common code.

The Store-delivery outbox must remain Antigravity-local until a second current route
uses identical ownership and both old loops are deleted. A shared shell added before
those deletions would be another wrapper, not simplification.

## 9. Command is a compatibility matrix, not a donor for the common shell

`crates/swarm-adapter-command/src/module_host.rs` selects among Batch v3/v4/v5 and
ACP v1/v2, accepts alternative exact capability sets, recognizes legacy/normalized
schema combinations and repeats full negotiation equality. The adapter also has its
own large journal and ACP queue/session lifecycle.

Before using Command to prove any shared abstraction, decide whether batch and ACP are
both retained products, select one current profile for each retained product, and
delete historical profiles and native method aliases. Otherwise the common shell
will simply encode Command's legacy matrix.

## 10. ACPX confirms ownership semantics, not a new framework

ACPX's shared runtime demonstrates useful behavior:

- one running queue owner for a named session;
- no fallback direct connection for shared controls, because that could create a
  competing provider session;
- queue acceptance differs from native prompt start and terminal result;
- cancellation targets the exact request only after acceptance;
- client detach does not cancel admitted work;
- a lost submission remains uncertain.

Its `requestId` is queue/status identity, not a general idempotent replay key. ACPX may
be evaluated only as a whole optional backend replacing Eliot's equivalent ACP owner.
Do not layer its flows/session product beside the existing adapter or import its
workflow language.

## 11. Tooling became another product

Measured current surfaces include:

| Surface | Approximate file size |
| --- | ---: |
| `.github/workflows/rust.yml` | 92 KB |
| `tools/qualification/New-NativeQualification.ps1` | 193 KB |
| `Invoke-CoreFailureQualification.ps1` | 124 KB |
| `Invoke-R48SchedulerQualification.ps1` | 109 KB |
| `tools/ci/build-module-package.ps1` | 30 KB |
| `Build-SwarmFrontendProvenance.ps1` | 20 KB |
| `SwarmBuildProvenanceHelpers.ps1` | 14 KB |
| `Test-InstallFrontendLegacyCompat.ps1` | 24 KB |

`New-NativeQualification.ps1` verifies not only selected images and behavior but also
source commit/tree, Cargo manifests and lockfile, toolchain files, rustc/cargo verbose
versions and even a SHA of the absolute target-cache path. It embeds exact artifact
versions, capability/schema arrays, preview hashes, refresh proof gates and provider
catalog body hashes.

`Install-ModuleArtifact.ps1` duplicates generic descriptor validation and adds exact
Antigravity artifact-specific contracts. Normal CI invokes legacy frontend inventory.
The workflow itself contains a long list of incident-specific focused modes and
custom attribution/classification machinery.

This does not improve product compatibility. A functional host and adapter may be
built from different clean checkouts/toolchains and still speak the same current
protocol. Build provenance is diagnostic; behavior is qualified through the public
API.

Keep actual installation safety:

- selected file/package byte verification once;
- private credentials;
- path containment and reparse protection where needed;
- bounded regular files and atomic publication;
- explicit protocol/action requirements.

Remove build-cache/compiler/checkout equality, per-artifact validators, duplicated
schema arrays, retired frontend inventory and behavioral proof packets from
qualification.

## 12. CI duplicates Cargo and product qualification

`tools/ci/package-scope.ps1` implements its own path classifier, Cargo metadata graph
resolution, target validation and documentation/tooling validation. `rust.yml` then
contains separate jobs and a large workflow-dispatch taxonomy.

The ordinary gate needs only:

- changed Markdown/link/whitespace validation;
- rustfmt on changed Rust;
- `cargo check` and warnings-denied Clippy for affected packages and real local
  reverse dependencies from `cargo metadata`;
- changed JS syntax;
- focused connected tests for a closed product slice.

Full native qualification belongs to explicit release/manual runs. Incident-specific
modes should become normal permanent behavioral tests or be deleted.

## 13. Execution duplication remains the largest runtime reduction

| Runtime | Current parallel surface | Target |
| --- | --- | --- |
| Antigravity | Node and Rust warm-stream implementations | Rust only after consumers/package paths switch |
| OpenCode | built-in host executor plus standalone Rust adapter | standalone Rust only |
| Codex | current Rust plus retired Python executor/SDK | Rust only; retain only concrete historical readers |
| Muse | Node bridge and Rust adapter | select one by complete behavior and total owned code |
| Claude | Rust/SDK path plus retained Node generations | one current Rust adapter with thin SDK driver |
| Command | Batch profile ladder and ACP profile ladder | current retained product profiles only |

Historical records do not justify keeping an executor. Concrete unsettled work gets a
finite disposition before deletion.

## 14. Passive observation still needs source changes

The required Governor view is not yet a joined local projection:

- `agent.list` remains binding-based;
- unmatched provider sessions are not first-class inventory;
- process owner identities are lost before Store projection;
- process sampling repeatedly hashes executables;
- restart can preserve stale connection health;
- follow/timeline loses source identity;
- GitHub polling focuses on Issues rather than independent remote refs.

Use existing adapter readers, host process/Git owners and Store observation. Do not
add a provider call per viewer or a second transcript database. Preserve unknown and
coverage gaps; never infer death/completion from age, low CPU, mtime or PID alone.

## 15. Store, automation and product breadth remain excessive

Confirmed targets remain:

- most reads serialized through one writer;
- base SQL text SHA can reject a semantically compatible database before migration;
- automation owner/name is part of identity and transfer relocates multiple journals;
- overlapping schedulers and `legacy_optional_workers` split lifecycle ownership;
- capacity and automation retain indexed state in encoded `meta` keys;
- direct acceptance and automatic acceptance use different review prerequisites;
- launch preview hashes and MCP/proof readiness block direct work;
- Concilium, integration ratification/ACK and automatic publication are separate
  product engines outside the compact core;
- host links worker implementations instead of only shared contracts.

Implementation names the replacement and deletion order. Do not create a second issue
program from this list.

## 16. Claims explicitly rejected

- Exact MCP exposure already has one owner; do not delete it as a duplicate
  permission engine.
- Schemars is not a proven net deletion for the inspected dynamic MCP inputs.
- Octocrab is optional; adding it beside `gh` would increase code.
- Result-page retention before Store ACK is necessary; the defect is that some routes
  keep it only in memory.
- A local transcript, alive PID or recent mtime does not settle an uncertain native
  write or prove Task completion.
- File moves, more crates or a Rust rewrite do not simplify retained responsibility.
- A shared abstraction with one implementation caller is preparation debt.

## Implemented source reduction

Commit `25a8e3b` removed the host's duplicate application CLI and MCP launcher:
`main.rs` fell from 1,949 to 743 physical lines, net −1,206 Rust lines. Public commands
remain in `swarm-cli`; host daemon/admin/internal worker entrypoints remain. No later
runtime deletion has landed yet.
