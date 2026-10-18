# Build, install and qualify Eliot components

This runbook separates the **current scripts** from the target package path. Current
scripts remain usable during migration, but their provenance matrices and
artifact-specific validators are not architecture. The target is implemented
route-by-route under [implementation](implementation.md#11-build-install-ci-and-qualification-reduction).

Installation does not prove a successful model turn. Provider CLI/service, account,
model and credentials are configured separately.

## 1. Target package contract

One current package contains:

```text
package-manifest.json
bin/<executable>
module-descriptor.json
resources/<required files only>
```

The manifest names:

- package/module coordinate for inventory;
- current protocol major;
- required semantic actions;
- executable and required resource relative paths;
- selected byte digests used once during installation/launch;
- optional build commit/toolchain as diagnostics only.

The descriptor contains launch/lifecycle/configuration and semantic actions. It does
not duplicate command/event schema inventories or create behavior branches from
artifact version.

The generic installer:

1. requires an absolute explicit install root;
2. rejects traversal through reparse/symlink targets where the platform check exists;
3. validates bounded regular package files and protected credential references;
4. verifies selected package bytes once;
5. atomically publishes one package directory;
6. returns the installed descriptor and executable identity;
7. does not launch, register a route or contact a provider.

Supervisor registration validates the installed package and current protocol/actions.
Runtime hello validates authenticated binding, protocol major and required action
subset. It does not compare source checkout, compiler, Cargo.lock, target-cache path
or a full echoed descriptor.

## 2. Current build command

Until the generic package path replaces the current scripts, build one selected Cargo
binary into one external shared target:

```powershell
$Target = 'D:\build-cache\eliot-shared-target'
cargo build --locked --release --target-dir $Target `
  --package swarm-antigravity-adapter --bin swarm-antigravity
```

Use the corresponding package/target for another retained component. Build
parallelism follows machine/Cargo capacity, not the number of agents. Do not allocate
a separate target directory per worker.

The current owner helper is built from `swarm-process` as
`swarm-module-owner`. It remains a separate selected executable until process
ownership is simplified.

## 3. Current adapter inventory

| Runtime | Current primary Rust package | Current descriptor source | Direction |
| --- | --- | --- | --- |
| Antigravity | `swarm-antigravity-adapter` | `modules/antigravity-rust/module-descriptor.template.json` | active coordinate is v6; converge route/tooling selectors and retire Node after consumers switch |
| OpenCode | `swarm-adapter-opencode` | `crates/swarm-adapter-opencode/registration/descriptor.template.json` | standalone Rust only |
| Codex | `swarm-adapter-codex` | package descriptor | Rust app-server only |
| Muse | `swarm-adapter-muse` plus Node bridge | current candidates | select one complete executor before installation is normalized |
| Claude | `swarm-adapter-claude` | `modules/claude-rust/module-descriptor.template.json` | one Rust adapter with thin SDK driver |
| Command | `swarm-adapter-command` | batch and ACP templates | first delete historical profiles; keep only retained current products |

Do not hardcode promotional model names into package or qualification policy. Models
belong to route configuration and provider-native discovery.

### Antigravity coordinate warning

The shipped Rust constant and descriptor are version `6`. Current route example,
package policy, native qualification and narrow installer validator still name `5`.
Do not create another version. Synchronize active selectors to v6 in one source slice;
leave historical v5 remediation receipts unchanged. After concrete v5 bindings are
disposed, remove active `5 | 6` branches.

## 4. Current transitional installer

The current command remains:

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

Review the preview, then repeat without `-WhatIf`.

Current behavior creates a coordinate-hashed directory and several receipt/coordinate
files. It also contains artifact-specific Antigravity and Node validators. This is a
migration constraint, not the target. Do not add another per-artifact validator.

Keep during migration:

- absolute install root outside checkout/vendor data;
- regular-file and path-containment checks;
- protected credentials rather than secret literals;
- atomic publication and exact installed byte verification.

Delete when the generic package installer is connected:

- per-artifact capability/schema validators;
- multiple hashes of the same local metadata used only for self-comparison;
- coordinate identity based on behavior version;
- duplicate Forge/worker installers;
- checks of retired sibling frontend resource trees.

## 5. Host configuration and route selection

The module supervisor receives installed descriptor paths and the owner helper:

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

Provider tokens do not belong in descriptor literals. Route configuration selects
workspace and optional native model/options. The descriptor/catalog is the source of
module and active package identity; do not maintain another version table in this
runbook.

Example:

```toml
[[routes]]
alias = 'antigravity-local'
runtime = 'antigravity'
module_artifact_id = 'eliot-antigravity.rust-headless.1'
enabled = true
[routes.native_options]
workspaceRoot = 'D:\work\repository'
reasoningEffort = 'high'
```

After startup, read `module.catalog.get` and select the exact current catalog entry.
Selection affects future bindings only. A route descriptor or capability does not
grant Store rights and does not prove a successful native turn.

## 6. Frontends and host chain

Public components are `swarm` (`swarm-cli`), `swarm-mcp` and optional
`swarm-gateway`. Host chain is `swarm-host`, `swarm-kernel-host` and
`swarm-supervisor`.

The current frontend provenance builder requires clean checkout/build manifests and
the installer inventories historical sibling resources. Target packaging uses the
same generic package rules as adapters:

- selected binaries/resources;
- current host IPC protocol requirement;
- atomic install;
- no requirement that CLI, launcher, runtime and supervisor share source commit,
  compiler or cache directory.

A release may record build provenance for diagnosis. Runtime compatibility depends on
the current protocol and behavior, not identical build environment.

## 7. Qualification

Qualification must exercise the installed product through public interfaces:

```text
start isolated host
create authorized manager
select installed route
create/claim Task
open session
submit one input
observe admitted/native/terminal state
retrieve exact result or honest unknown
shutdown without replay
```

The receipt records selected image/package identity and observable outcomes. It does
not claim account/model consumption unless the provider source supplies it.

Remove from qualification as their migrated route permits:

- host/module source commit/tree equality;
- Cargo.toml/Cargo.lock/toolchain/rustc/cargo equality;
- target-directory hash equality;
- exact artifact capability/schema arrays duplicated from Rust;
- mandatory preview plan digest;
- Antigravity refresh/proof prerequisite;
- provider catalog response-body hashes as dispatch authority;
- legacy frontend compatibility inventory.

A provider or transport timeout leaves the operation unknown and forbids replay. A
failed qualification does not kill or reconfigure an externally owned harness.

## 8. CI

Normal CI should remain small:

1. changed Markdown/link/whitespace validation;
2. rustfmt for changed Rust;
3. `cargo check` and warnings-denied Clippy for affected packages and local reverse
   dependencies derived from `cargo metadata`;
4. `node --check` for changed JavaScript;
5. connected focused tests when a product slice closes.

Full native/product qualification is a manual/release gate. Do not add another
workflow-dispatch mode for each incident. Convert a lasting regression into a normal
behavioral test, then delete the focused mode and its script branch.

## 9. Current standalone workers

Use the existing generic `Install-StandaloneWorker.ps1` for retained standalone
workers. The separate Forge installer is a duplicate to remove. Script, bus, checks
and automation executables remain optional selected components; installing an image
does not create Store admission or execution rights.

Each retained component needs one current package path and one current configuration
owner. Historical build manifests and executable versions do not justify keeping
parallel installers or runtime branches.
