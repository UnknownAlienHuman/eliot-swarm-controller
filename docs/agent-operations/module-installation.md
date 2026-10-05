# Install and select a local module

This runbook covers the four Rust adapters and the local observer/check executor settings in the current controller config. It installs adapter executables only. It does not install a provider, native service, model, or vendor CLI, and installation or route selection does not qualify those native systems.

## Build and install one adapter

Build only the selected package, using a caller-owned shared target directory. The package/binary pairs are:

| Adapter | Cargo package | Binary | Descriptor template |
|---|---|---|---|
| Codex | `swarm-adapter-codex` | `swarm-codex-adapter` | `crates/swarm-adapter-codex/module-descriptor.template.json` |
| OpenCode | `swarm-adapter-opencode` | `swarm-adapter-opencode` | `crates/swarm-adapter-opencode/registration/descriptor.template.json` |
| Command | `swarm-adapter-command` | `swarm-adapter-command` | `crates/swarm-adapter-command/module-descriptor.template.json` |
| Antigravity | `swarm-antigravity-adapter` | `swarm-antigravity` | `modules/antigravity-rust/module-descriptor.template.json` |

```powershell
$Manifest = 'C:\src\eliot-swarm-controller\crates\swarm-adapter-command\Cargo.toml'
$Package = 'swarm-adapter-command'
$Target = 'D:\build-cache\eliot-shared-target'
cargo build --manifest-path $Manifest --package $Package --release --locked --target-dir $Target
```

Use the resulting `$Target/release/<binary>.exe` as the installer source. Build the owner helper separately from `crates/swarm-process/Cargo.toml` (`--package swarm-process --bin swarm-module-owner`) and keep its installed absolute path and lowercase SHA-256 for local host config.

The host writes the private scoped launch plan and resolver map below its storage data directory after durable module demand. It invokes the pinned helper as `swarm-module-owner <absolute-plan-path> <absolute-resolver-map-path>`. The helper accepts exactly these two paths; its per-launch files are produced by the host.

Copy the selected descriptor template outside the checkout. Set `enabled` to `true` only when ready to make that version selectable, and replace any `<INSTALLER_CREDENTIAL_FILE_REF>` with an opaque protected-reference name. Keep adapter identity, protocol, schema, command/event schemas, capabilities, lifecycle and restart policy aligned with that adapter's template. Do not put credential bytes or provider tokens in the descriptor. The installer fills `launch.executable` and `launch.executable_sha256` from the built file.

Use an existing dedicated absolute install root outside the controller/Codex/OpenCode trees. The root must already exist, remain within the installer's path limit, and contain no reparse-point traversal. Preview first, then repeat without `-WhatIf`:

```powershell
$Source = 'D:\build-cache\eliot-shared-target\release\swarm-adapter-command.exe'
$Descriptor = 'D:\release-input\command-descriptor.json'
$InstallRoot = 'D:\eliot\installed-modules'
$Installer = 'C:\src\eliot-swarm-controller\tools\modules\Install-ModuleArtifact.ps1'
pwsh -NoProfile -File $Installer -SourceExecutable $Source -DescriptorTemplate $Descriptor -InstallRoot $InstallRoot -WhatIf
# After reviewing the preview, run the same command without -WhatIf.
```

The installer creates a coordinate-hashed directory with `module.exe`, `module-descriptor.json`, `install-receipt.json`, and local `install-coordinate.json`. It never registers or launches anything and never replaces different bytes at an existing identity. Use a new artifact version for changed executable or descriptor bytes. At host startup, the optional supervisor loads only configured descriptor files, checks the descriptor/receipt/executable hashes, then registers through its reserved local identity. Managers do not receive that credential. A descriptor template being installed is not a registration receipt; registration is also not a route selection.

## Configure the host and route

The `[module_supervisor]` table is disabled by default. When enabling it, use absolute paths (or paths resolved relative to the controller TOML), name each installed descriptor explicitly, and pin the built owner helper:

```toml
[module_supervisor]
enabled = true
install_root = 'D:\eliot\installed-modules'
descriptor_files = [
  'D:\eliot\installed-modules\artifact-<coordinate-sha256>\module-descriptor.json',
]
owner_helper = 'D:\eliot\bin\swarm-module-owner.exe'
owner_helper_sha256 = '<64-lowercase-hex-sha256>'
protected_files = {}
route_config_mapper = 'descriptor_schema'
```

`protected_files` maps only additional descriptor-declared protected references to existing local files. The binding credential reference in `launch.credential_ref` is provisioned and resolved by the host; do not add a provider key or credential content to this table. The default `descriptor_schema` mapper gives descriptors with no config schema no extra launch values; OpenCode's route options are bridged only by its exact registered schema ID/version/digest.

Each route must use the exact Rust artifact ID and workspace field enforced by `src/config.rs`:

| Route | `runtime` | `module_artifact_id` | `workspace_option` | `native_options` |
|---|---|---|---|---|
| Codex | `codex` | `codex-rust-controller.1` | `workspaceRoot` | exactly `modelProvider`, `model`, `workspaceRoot` |
| OpenCode | `module` | `eliot-opencode-v2.rust-http.1` | `directory` | `service_id`, `connection_file`, `expected_version`, `directory`, and `model = { id, providerID, variant }` |
| Command | `command` | `eliot-command.rust-headless.1` | `workspaceRoot` | exactly `modelId`, `workspaceRoot` |
| Antigravity | `antigravity` | `eliot-antigravity.rust-headless.1` | `workspaceRoot` | `modelId = 'gemini-3.8-flash-high'`, `workspaceRoot`; optional `reasoningEffort`, `agent`, `dangerouslySkipPermissions` |

For example, the route tables use the normal controller TOML shape:

```toml
[[routes]]
alias = 'command-local'
runtime = 'command'
module_artifact_id = 'eliot-command.rust-headless.1'
enabled = true
workspace_option = 'workspaceRoot'
[routes.native_options]
modelId = 'REPLACE_WITH_YOUR_NATIVE_MODEL_ID'
workspaceRoot = 'D:\work\your-repository'
```

Set `enabled` only for routes you want to admit new work on; omitted route `enabled` defaults to `false`. Keep the adapter's private host/config file and native executable location at the path named by its descriptor `argv`; the installer copies neither. Use the adapter's own README for that file's exact fields. Existing native services/endpoints, access, models, and provider credentials remain operator-managed and must be independently qualified.

After restarting with the local config, read `module.catalog.get`, note `catalog_revision`, and select the exact enabled descriptor for your authenticated Manager identity (Operator identity is also accepted):

```json
{
  "route_alias": "command-local",
  "module_id": "runtime.command",
  "artifact_id": "eliot-command.rust-headless.1",
  "version": "1",
  "expected_catalog_revision": 12
}
```

Call `module.route.select` with that object. Use the exact values and revision returned by the catalog; a stale revision must be reread. Selection applies only to that identity's future bindings. It does not change existing bindings or start a worker. A later admitted pending Operation creates module demand; the host provisions and verifies the binding-scoped IPC credential, then launches the adapter under the descriptor and owner-helper checks. `external_attach` on Codex/OpenCode does not grant control of their native service. `owned_service` on Command/Antigravity describes the adapter module lifecycle; it does not install or qualify their native CLI.

## Package and install the Forge worker

Build `swarm-forge-worker` from a clean, committed checkout with
`tools/ci/build-module-package.ps1 -Package swarm-forge-worker -Profile release`,
supplying the existing shared `-TargetDir` and an explicit package `-OutputDir`.

Local Cargo recipes also require an explicit shared target outside the checkout.
Pass its absolute path as the recipe's `TARGET` argument, or set
`CARGO_TARGET_DIR` for that invocation. The scoped `Verify` and manual `FullRust`
entrypoints validate `-TargetDir` (or the same environment variable) before
compiling. Both Clippy and tests use that one cache; neither stage allocates a
separate default `target` for each checkout.
The output contains `bin/swarm-forge-worker.exe` and `build-manifest.json`.

Pass that absolute executable path and the chosen host executable to
`tools/modules/Install-ForgeWorker.ps1 -HostExecutable <absolute-swarm.exe>
-PackageExecutable <absolute-worker.exe>`. `-WhatIf` previews placement.
The installer validates the exact package manifest, clean source revision,
binary target, length and SHA-256, then places the worker beside that host.
Identical installed bytes are a no-op; different bytes are not overwritten.
It does not configure or launch either process. The unsigned build manifest
provides consistency evidence; it is not a signature.

## Build and install the standalone ScriptRun worker

Build package `swarm-script-worker` with the existing module package builder,
using the same external shared Cargo target directory and a new explicit output
directory. Pass its `bin/swarm-script-worker.exe` to
`tools/modules/Install-ScriptWorker.ps1 -PackageExecutable <absolute-worker.exe>
-InstallDirectory <existing-absolute-directory>`. Use `-WhatIf` to preview.
The installer verifies the exact release package, clean source manifest, length
and image digest. Identical installed bytes are a no-op; different bytes are
not overwritten. It returns the absolute path and digest without editing config.

Configure the returned values explicitly:

```toml
[scripts.executor]
executable = 'C:\ELIOT\artifacts\swarm-script-worker.exe'
sha256 = '<64-lowercase-hex-digest-returned-by-installer>'
artifact_id = 'swarm-script-worker.1'
version = '0.1.0'
```

The pin defaults absent. New receipts retain a selected pin; older receipts
keep their admitted backend. The standalone worker starts after Store admission,
reports readiness and waits for Store Go before launching the interpreter.
It cannot apply controller effects. An invalid selected image cannot fall back.

## Package the host with source provenance

`tools/ci/Build-SwarmHostProvenance.ps1 -TargetDir <existing-shared-target>
-OutputDir <new-package-directory>` manually builds only the release host package
through the existing builder. It emits `bin/swarm.exe` and the source/build/image
manifest used to bind a qualification run to an exact artifact. Normal source
pushes do not invoke this release entrypoint. The caller supplies the existing
shared target directory; no separate worker or worktree build cache is required.

## Optional checks and local observer

The manual `module-package.yml` workflow offers eleven actual executable
packages. Select one package and profile; `eliot-swarm-controller` requires
`release`. All choices use one runner shared target directory and produce one
package manifest. Library-only supervisor is not an executable selection.
For current-source native qualification, use
`tools/qualification/New-NativeQualification.ps1` with explicit host/module
build-manifest and image hashes, installed descriptor and private configuration.
The harness uses a fresh DataRoot, protects unrelated/current Codex processes
and sends a native input once. Unknown effects use bounded readback rather
than another send. Source/manifest consistency is recorded separately from
the actual runtime result.

`tools/qualification/Invoke-CoreFailureQualification.ps1` uses an explicitly
pinned host and a fresh private DataRoot for lost-caller-ACK, request-conflict and
optional HookSource deduplication checks. Its independent Manager readback must
prove admission before graceful restart. It never treats a missing caller reply
as a native-effect unknown outcome, injects a vendor effect, or stops another
host. Source availability and AST validation do not establish a passing run.

The checks executor is a separate optional process pin, not a module descriptor. Build package `swarm-checks` and configure its binary using `[checks.executor]`. Checks default disabled, and the executor pin defaults absent:

```toml
[checks.executor]
executable = 'D:\eliot\bin\swarm-checks.exe'
sha256 = '<64-lowercase-hex-sha256-of-that-file>'
artifact_id = 'swarm-checks'
version = '0.1.0'
```

The host verifies the executable path and image digest, including before Store Go. `artifact_id` and `version` are retained evidence labels; they are not read from binary metadata. A bad path or digest fails that launch without switching to the legacy worker.

The host `[observability]` recorder also defaults disabled. Its current fields are `enabled`, optional `directory` and `live_config_file`, `queue_records`, `queue_bytes`, `max_record_bytes`, `file_segment_bytes`, `retention_bytes`, and `retention_days`; omitted values use the current config defaults. Relative paths resolve beside the controller config. The optional pinned JSON live-config file sets diagnostic severity/category filters and retention when the lazy recorder runs. This is bounded metadata recording, not prompt, tool-argument, environment, or credential capture. The standalone `swarm-observer` binary takes an absolute private directory and optional `--queue-records`, `--queue-bytes`, `--max-record-bytes`, `--segment-bytes`, `--retention-bytes`, and `--retention-days`; it reads newline-delimited diagnostic records from stdin.
