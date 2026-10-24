# Build, install and qualify Eliot components

This runbook separates the current transitional scripts from the compact target.
[Implementation](implementation.md) owns migration order;
[status](../implementation-status.md) records actual code and evidence. Installation
or compilation alone never proves a working native turn.

## 1. One current package contract

A retained component has one current package:

```text
package-manifest.json
bin/<executable>
module-descriptor.json
resources/<required files only>
```

The manifest contains:

- inventory coordinate;
- current Eliot protocol major;
- required semantic actions;
- executable/resource relative paths;
- selected byte digests verified at install/launch;
- optional build provenance for diagnosis only.

The descriptor owns typed launch/lifecycle/configuration and required semantic
actions. It does not duplicate full command/event schema inventories or select Core
behavior from artifact version.

A generic installer:

1. requires an absolute explicit private install root;
2. rejects traversal and symlink/reparse escape where the platform supports the
   check;
3. validates bounded regular package files and protected credential references;
4. verifies selected bytes once;
5. atomically publishes one package directory;
6. returns installed descriptor/executable identity;
7. does not launch a model, register a route or contact a provider.

Supervisor registration validates the installed package. Runtime hello validates the
authenticated binding, one protocol major and the required semantic capability subset.
It does not compare source checkout, compiler, Cargo.lock or target-cache path and does
not echo the whole descriptor back to itself.

## 2. Selected current runtime direction

| Runtime | Selected target | Transition boundary |
| --- | --- | --- |
| Antigravity | Rust adapter, active coordinate v6 | A1b removed the dispatch refresh/proof gate. Finish A2–A3: add durable Store delivery, keep real actions only, then retire remaining Node profiles. |
| OpenCode | standalone `swarm-adapter-opencode` against one exact live-qualified current native contract | Freeze built-in new work, disposition concrete old bindings, delete host `runtime/opencode_v2`, legacy worker/MCP/service paths and universal journal machinery. |
| Codex | Rust app-server adapter over the current external app-server | Delete Python writable route, profile ladders, full checkpoint/event mirror and Goal proof machinery after finite disposition. |
| Muse | **Node bridge `.13` over the official low-level SDK** | This is the only new-work route during refactor. Finish/export concrete custom-Rust work, then remove `swarm-adapter-muse`, disabled descriptor, custom MSP wire/schema and Rust-only fixtures. Do not keep both or add the official Rust SDK as a third executor. |
| Claude | one Rust adapter with a thin driver for the selected official SDK/runtime | Retire old Node generations after current work is disposed. Advertise only capabilities verified on the selected installed SDK. |
| Command | one current Batch route and one stable ACP v1 route | Delete Batch/ACP artifact-profile ladders, generic journals and public `native.command.*` methods while preserving `session/load`, exact prompt/reply/cancel/close and option readback. |

Model names, account selection, effort and permission policy belong to route
configuration/native discovery, not package compatibility.

## 3. Current build path

Until the generic package path replaces the current scripts, build the exact selected
binary into one shared external target directory:

```powershell
$Target = 'D:\build-cache\eliot-shared-target'
cargo build --locked --release --target-dir $Target `
  --package swarm-antigravity-adapter --bin swarm-antigravity
```

Use the corresponding package/target for another retained component. Build
parallelism follows machine and Cargo capacity, not agent count. Do not allocate one
target directory per worker.

`swarm-module-owner` remains the current selected helper until process ownership is
simplified. Its existence does not justify repeating package/contract identity in
every runtime receipt.

## 4. Transitional installer

The current installer remains usable while the target package path is implemented:

```powershell
$Source = 'D:\build-cache\eliot-shared-target\release\swarm-antigravity.exe'
$Descriptor = 'D:\release-input\antigravity-descriptor.json'
$InstallRoot = 'D:\eliot\installed-modules'
pwsh -NoProfile -File tools/modules/Install-ModuleArtifact.ps1 `
  -SourceExecutable $Source `
  -DescriptorTemplate $Descriptor `
  -InstallRoot $InstallRoot `
  -WhatIf
```

Reviewing `-WhatIf` is useful for an operator installing packages, but normal agent
execution after installation needs no repeated approval. Repeat without `-WhatIf`
when performing the installation action.

Keep during migration:

- absolute install root outside checkout/vendor data;
- regular-file and path-containment checks;
- private credentials rather than descriptor secret literals;
- atomic publication and exact installed-byte verification.

Delete when the generic installer is connected:

- per-artifact capability/schema validators;
- coordinate/version branches that decide behavior;
- repeated hashes of local metadata used only for self-comparison;
- duplicate Forge/worker installers;
- retired sibling frontend inventory and compatibility fixtures.

Do not add another per-runtime installer or validator while deleting the existing
ones.

## 5. Host configuration and route selection

Supervisor configuration references installed descriptors and the current owner
helper. Provider secrets remain protected references.

```toml
[module_supervisor]
enabled = true
install_root = 'D:\eliot\installed-modules'
descriptor_files = [
  'D:\eliot\installed-modules\<package>\module-descriptor.json',
]
owner_helper = 'D:\eliot\bin\swarm-module-owner.exe'
owner_helper_sha256 = '<selected-file-digest>'
protected_files = {}
```

A route selects one installed module plus native account/service/model/effort/options,
permission policy, workspace and root capacity:

```toml
[[routes]]
alias = 'antigravity-local'
runtime = 'module'
module_artifact_id = 'eliot-antigravity.rust-headless.1'
enabled = true

[routes.native_options]
workspaceRoot = 'D:\work\repository'
reasoningEffort = 'high'
```

Catalog/descriptor provides current package identity. Route selection affects future
bindings only. It grants no Store rights and proves no native turn.

The active Antigravity source, descriptor, route example, package policy and native
qualification use version 6. Historical v5 receipts remain historical; they do not
keep an active v5 capability branch.

## 6. Frontends and host chain

Public components are `swarm`, `swarm-mcp` and optional `swarm-gateway`. Host chain is
`swarm-host`, `swarm-kernel-host` and `swarm-supervisor`.

Package frontends by selected binaries/resources and current IPC protocol. Do not
require CLI, launcher, host and supervisor to share source commit, compiler or cache
directory. Build provenance may be retained for diagnosis; behavior compatibility is
the current protocol and exercised capability.

## 7. Qualification through the public product

Qualification is an executable product path, not another proof document:

```text
start isolated installed host
create/use an authorized Manager agent identity
select installed route
create or select Task and workspace
open/resume native session
submit one logical input
observe admission, native progress and terminal/unknown state
retrieve exact result
exercise one question/reply when supported
restart/reconnect at the selected boundary
shutdown/release without input replay
```

The resulting receipt records selected package/image and observable outcomes. It does
not infer account/model consumption or provider success without native evidence.

Automate this path in CI or an owner-machine qualification runner where the real
provider/account/Windows environment exists. “Owner-machine” identifies the required
environment; it does not mean a human must perform each command. Humans provide local
credentials/access and decide unresolved product intent. The runner performs the
repeatable sequence and publishes exact evidence.

Remove from qualification as each migrated route permits:

- checkout/compiler/toolchain/cache equality;
- exact artifact capability/schema arrays duplicated from Rust;
- mandatory launch-preview digest;
- Antigravity refresh/proof prerequisite — removed in A1b;
- provider catalog response-body hashes as authority;
- legacy frontend/resource compatibility inventory;
- incident-specific workflow modes once a lasting regression test exists.

Timeout/transport loss leaves the exact Operation unknown and forbids guessed replay.
A failed qualification does not kill or reconfigure an externally owned harness.

## 8. CI during implementation

Normal construction checks stay small:

1. changed Markdown/link/whitespace validation;
2. rustfmt for changed Rust;
3. `cargo check` and warnings-denied Clippy for affected packages and actual local
   reverse dependencies derived from `cargo metadata`;
4. `node --check` for changed JavaScript;
5. one connected focused regression when a vertical behavior slice closes.

Broader installed/native/restart/load qualification runs when its connected product
path exists. Do not create a permanent workflow-dispatch mode per incident. Convert a
lasting failure into a normal test, then remove the one-shot workflow and patch
script.

## 9. Completion for a package slice

A package migration is complete only when:

- one current installed package owns new work;
- route/catalog/hello agree on the semantic actions actually implemented;
- old package/profile gets no new bindings;
- concrete unsettled work is finished, exported or explicitly re-attempted;
- replaced descriptor, installer rules, compatibility branches, fixtures and CI are
  deleted together;
- public qualification demonstrates the retained positive path and honest unknown
  outcomes;
- no normal runtime action has been replaced by an operator procedure.
