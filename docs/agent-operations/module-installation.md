# Install and select a local module

The commands below describe the **current scripts**, including their packaging restrictions. The target is one package with its selected descriptor/image and shared validation. Equal checkout revisions, compilers, dependency graphs or build-cache paths are not runtime compatibility requirements. Remove repeated provenance validators, retired OpenCode `node_modules` inventory and the duplicate Forge installer; these changes are **not yet implemented**. Concrete old data uses the [finite transition](refactoring.md#retire-executors-without-losing-history), not an obligation to preserve every installer or executable version.

This runbook installs controller components. Native services, vendor CLIs, models and provider credentials are configured separately. Installation does not launch them or establish a successful model turn.

## Build and install one adapter

Select models through route configuration and the provider catalog; use native defaults where an omitted model is supported. Do not hardcode promotional model names into code or checks.

| Adapter  |  Cargo package  |  Binary  |  Descriptor template |
| --- | --- | --- | --- |
| Codex  |  `swarm-adapter-codex`  |  `swarm-codex-adapter`  |  `crates/swarm-adapter-codex/module-descriptor.template.json` |
| OpenCode  |  `swarm-adapter-opencode`  |  `swarm-adapter-opencode`  |  `crates/swarm-adapter-opencode/registration/descriptor.template.json` |
| Command  |  `swarm-adapter-command`  |  `swarm-adapter-command`  |  `crates/swarm-adapter-command/module-descriptor.template.json` (Batch v5); `module-descriptor-acp.template.json` (ACP v2) |
| Antigravity  |  `swarm-antigravity-adapter`  |  `swarm-antigravity`  |  `modules/antigravity-rust/module-descriptor.template.json`; Node `modules/antigravity/module-descriptor.template.json` (`antigravity-cli-warm-bridge.4`) |
| Claude  |  `swarm-adapter-claude`  |  `swarm-adapter-claude`  |  `modules/claude-rust/module-descriptor.template.json` |

```powershell
$Manifest = 'C:\src\eliot-swarm-controller\crates\swarm-adapter-command\Cargo.toml'
$Package = 'swarm-adapter-command'
$Target = 'D:\build-cache\eliot-shared-target'
cargo build --manifest-path $Manifest --package $Package --release --locked --target-dir $Target
```

Use `$Target/release/<binary>.exe` as the installer source. Build the owner helper from `crates/swarm-process/Cargo.toml` with `--package swarm-process --bin swarm-module-owner`. Its installed path and SHA-256 are current host configuration inputs. The host creates the helper's two private launch-plan/resolver-map arguments itself.

Copy the selected descriptor template outside the checkout. Replace credential placeholders with a protected-reference name, never credential bytes. Keep its protocol, capabilities and launch arguments aligned with the adapter. The installer fills `launch.executable` and `launch.executable_sha256` from the built file.

For Command Batch 5 / ACP 2, preserve the typed `module_host_config_path` marker. Replace only the final `--config` argument with the absolute native Command JSON path described in the [adapter README](../../crates/swarm-adapter-command/README.md). That file selects the native executable/mod; route options select a model override. The host supplies binding credentials through its generated private config.

Use an existing absolute install root outside the controller and vendor trees, without reparse-point traversal. `-WhatIf` previews the current installer:

```powershell
$Source = 'D:\build-cache\eliot-shared-target\release\swarm-adapter-command.exe'
$Descriptor = 'D:\release-input\command-descriptor.json'
$InstallRoot = 'D:\eliot\installed-modules'
$Installer = 'C:\src\eliot-swarm-controller\tools\modules\Install-ModuleArtifact.ps1'
pwsh -NoProfile -File $Installer -SourceExecutable $Source -DescriptorTemplate $Descriptor -InstallRoot $InstallRoot -WhatIf
# After reviewing the preview, run the same command without -WhatIf.
```

**Current limitation:** the installer creates a coordinate-hashed directory with `module.exe`, `module-descriptor.json`, `install-receipt.json` and `install-coordinate.json`; changed bytes under that identity are rejected. This is existing behavior to simplify, not a requirement to mint a new identity for every edit. Installation does not register or launch the module. Keep the native Command config separately available at its configured path.

## Package independent frontends

The packages are `swarm-mcp`, `swarm-gateway` and `swarm-cli`; the latter emits `swarm.exe`. Gateway is optional. The host chain is `swarm-host` (launcher), `swarm-kernel-host` (Store/IPC) and `swarm-supervisor`.

The current frontend builder requires a clean committed checkout, an existing external target and a fresh output directory. It emits the executable and `build-manifest.json`; the installer requires that manifest and rejects differing bytes already installed at the destination.

```powershell
$TargetDir = 'D:\build-cache\eliot-shared-target'
$PackageDirectory = 'D:\eliot\packages\swarm-mcp-new-version'
pwsh -NoProfile -File tools/ci/Build-SwarmFrontendProvenance.ps1 `
  -Package swarm-mcp -TargetDir $TargetDir -OutputDir $PackageDirectory
pwsh -NoProfile -File tools/modules/Install-SwarmFrontend.ps1 `
  -PackageDirectory $PackageDirectory -InstallDirectory 'D:\eliot\frontend-version' -WhatIf
```

The install directory must already exist. Repeat without `-WhatIf` to install. Current sibling checks compare protocol, target and launcher arguments. The same installer also inventories historical resource receipts, including the retired OpenCode dependency tree; removing that unrelated prerequisite is part of the target above.

## Configure the host and route

`module_supervisor` defaults disabled. Configure installed descriptor paths and the owner helper:

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

`protected_files` maps additional descriptor references to local files. The host provisions `launch.credential_ref`; provider tokens do not belong in this table. The default mapper uses the descriptor's registered configuration schema.

Set `module_artifact_id` from the selected descriptor's `artifact.artifact_id`;
read its version from the installed catalog when selecting a route. The descriptor
is the source for those values, not another release matrix in this runbook.

| Route | `runtime` | `workspace_option` | `native_options` |
| --- | --- | --- | --- |
| Codex | `codex` | `workspaceRoot` | `workspaceRoot`; optional explicit `modelProvider` and `model` |
| OpenCode | `module` | `directory` | `service_id`, `connection_file`, `directory`; optional explicit `model = { id, providerID, variant }` |
| Command | `command` | `workspaceRoot` | `workspaceRoot`; optional explicit `modelId` |
| Antigravity | `antigravity` | `workspaceRoot` | `workspaceRoot`; optional explicit `modelId`, `reasoningEffort`, `agent`, `dangerouslySkipPermissions` |
| Claude | `module` | `workspaceRoot` | `workspaceRoot`; optional explicit `modelId` |

The current OpenCode descriptor uses `opencode-v2-native-options@4`. Its external attach route has no mandatory server `expected_version`; compatibility is determined by protocol capability. See [configuration](configuration.md) for connection fields and [donor boundaries](donor-map.md) for native SDK ownership.

```toml
[[routes]]
alias = 'command-local'
runtime = 'command'
module_artifact_id = 'eliot-command.rust-headless.1'
enabled = true
workspace_option = 'workspaceRoot'
[routes.native_options]
workspaceRoot = 'D:\work\your-repository'
```

Route `enabled` defaults false. After starting with this configuration, read `module.catalog.get` and call `module.route.select` using its actual values:

```json
{
  "route_alias": "command-local",
  "module_id": "runtime.command",
  "artifact_id": "eliot-command.rust-headless.1",
  "version": "5",
  "expected_catalog_revision": 12
}
```

The example revision is a placeholder; reread a stale catalog. Selection belongs to the authenticated Manager (Operator is also accepted) and affects future bindings. Admitted work creates module demand and supplies binding credentials. An adapter's `owned_service` lifecycle does not confer ownership of the vendor CLI. Result association and unavailable historical bodies follow the [result contract](architecture.md#41-deterministic-turn-scoped-result-correlation-agentresult); descriptor capability names alone do not prove implementation.

## Hot descriptor installation (Operator-only, no live restart)

`module.descriptor.install` takes `{ "descriptor_file": "<absolute path>" }`. The current local Operator path checks the installed receipt, descriptor and executable, extends the catalog and retains the path for supervisor restart. Active bindings continue unchanged; Managers can then select the new entry. Source owners are `load_installed_descriptor`, `register_installed_descriptor` and `extend_descriptor_catalog` in `swarm-supervisor`. Shared schema validation belongs to `swarm-contracts::module_catalog`; the duplicate PowerShell validator is part of the simplification.

## Package executable workspace coordinates

Current `tools/ci/build-module-package.ps1` uses `tools/ci/module-package-policy.json` and Cargo metadata to select a declared binary. It rejects additional binary targets and records the workspace-resolved dependency graph. Those restrictions remain in the script; they are not the target design for independent components. The manual `module-package.yml` workflow calls the same builders and selects one package per run.

```powershell
just package-module swarm-bus release `
  'D:\build-cache\eliot-shared-target' 'D:\artifacts\swarm-bus-release'
```

For worker packages below, use `-Profile release`, an external shared `-TargetDir` and a fresh `-OutputDir` with that builder. Output is `bin/<binary>.exe` plus `build-manifest.json`. Current installers require that manifest, verify the executable and return its installed path/digest. Identical bytes are a no-op; differing bytes are rejected. Installation does not configure or launch work.

## Build and install the standalone automation worker

```powershell
$TargetDir = 'D:\build-cache\eliot-shared-target'
$HostOutput = 'D:\release\swarm-host'
$WorkerOutput = 'D:\release\swarm-automation-worker'
pwsh -NoProfile -File .\tools\ci\build-module-package.ps1 `
  -Package eliot-swarm-controller -Profile release `
  -TargetDir $TargetDir -OutputDir $HostOutput
pwsh -NoProfile -File .\tools\ci\build-module-package.ps1 `
  -Package swarm-automation -Profile release `
  -TargetDir $TargetDir -OutputDir $WorkerOutput

$PackageExecutable = Join-Path $WorkerOutput 'bin\swarm-automation-worker.exe'
$Installer = '.\tools\modules\Install-StandaloneWorker.ps1'
$installed = & $Installer `
  -WorkerCoordinate swarm-automation-worker `
  -HostExecutable 'C:\Program Files\Eliot Swarm\swarm-host.exe' `
  -PackageExecutable $PackageExecutable -WhatIf
$installed
```

The selected host must already exist. This places the worker beside it; the returned `image_sha256` identifies the worker independently of the host image.

## Package and install the Forge worker

Build `swarm-forge-worker`; pass `bin/swarm-forge-worker.exe` to `Install-StandaloneWorker.ps1` with `-WorkerCoordinate swarm-forge-worker`, `-HostExecutable <absolute-host.exe>` and `-PackageExecutable <absolute-worker.exe>`. The separate `Install-ForgeWorker.ps1` is a duplicate to remove, not an interface that future refactoring must preserve.

## Build and install the standalone ScriptRun worker

Build `swarm-script-worker`. Pass `bin/swarm-script-worker.exe` to `tools/modules/Install-ScriptWorker.ps1 -PackageExecutable <absolute-worker.exe> -InstallDirectory <existing-absolute-directory>`; add `-WhatIf` to preview. Configure its returned values:

```toml
[scripts.executor]
executable = 'C:\ELIOT\artifacts\swarm-script-worker.exe'
sha256 = '<64-lowercase-hex-digest-returned-by-installer>'
artifact_id = 'swarm-script-worker.1'
version = '0.1.0'
```

The executor defaults absent. It waits for Store admission/readiness/Go before starting the interpreter. A failed selected image does not choose another backend.

## Build and install the managed bus dispatcher

Build `swarm-bus`. Pass `bin/swarm-bus-dispatcher.exe` to `tools/modules/Install-BusDispatcher.ps1 -PackageExecutable <absolute-dispatcher.exe> -InstallDirectory <existing-absolute-directory>`; add `-WhatIf` to preview.

```toml
[bus_supervisor]
enabled = true
dispatcher_executable = 'C:\ELIOT\artifacts\swarm-bus-dispatcher.exe'
dispatcher_sha256 = '<64-lowercase-hex-digest-returned-by-installer>'
```

The supervisor starts it only for a managed, ready Store registration. Installing the image and setting configuration do not create that registration.

## Package runtime processes with source provenance

Current wrappers are `Build-SwarmHostProvenance.ps1` for `swarm-host`, `Build-SwarmKernelHostProvenance.ps1` for `swarm-kernel-host`, and `Build-SwarmSupervisorProvenance.ps1` for `swarm-supervisor`. They take `-TargetDir` and a fresh `-OutputDir` and call the shared builder. Normal source pushes do not invoke these manual release builders.

The current Native/Core qualification scripts separately require image and build-manifest digests. Native qualification additionally rejects host/module differences in source, toolchain and even the build-cache path (`New-NativeQualification.ps1`, `HOST_MODULE_BUILD_PROVENANCE_DIVERGED`). Remove that prerequisite: a functional turn checks selected images, protocol and result, not identical build environments. No harness run is needed to prepare this source change.

## Optional checks and local observer

Build `swarm-checks` and configure its executable. Checks default disabled; the executor defaults absent:

```toml
[checks.executor]
executable = 'D:\eliot\bin\swarm-checks.exe'
sha256 = '<64-lowercase-hex-sha256-of-that-file>'
artifact_id = 'swarm-checks'
version = '0.1.0'
```

The host verifies the selected image before Store Go. `artifact_id` and `version` are configuration labels, not executable metadata. Recorder settings belong in [observability](observability.md). The standalone `swarm-observer` reads diagnostic NDJSON from stdin and takes an absolute private directory plus optional queue, record-size, segment-size and retention bounds.
