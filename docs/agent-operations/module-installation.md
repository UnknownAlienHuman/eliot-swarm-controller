# Install and select a local module

This runbook covers the standalone adapter packages, independent frontend binaries, and local observer/check executor settings in the current controller config. It installs adapter executables only. It does not install a provider, native service, model, or vendor CLI, and installation or route selection does not qualify those native systems.

## Build and install one adapter

New OpenCode and Command Code qualification runs use
`inclusionai/ling-3.1-flash`. Pass that exact reference to
`New-NativeQualification.ps1 -OpenCodeCommandTestModelRef inclusionai/ling-3.1-flash`
as a selector. For OpenCode, the configured route is accepted only when its
actual `id` equals that full reference or its exact `providerID/id` composite
equals it; the harness records the route's actual provider and model and never
infers a provider from the reference. The launch fixture's `requested_model`
remains the route's actual `id`, matching Store launch admission. Before
creating a native session, `agent.open` reads the current OpenCode model catalog and checks that exact
provider/model/variant. If the native catalog does not contain the selected
model, the operation fails before the harness sends its single task input.
Bunny is disabled for new runs; historical receipts retain their original
model identity. Command retains exact whole-string model matching. Claude's
version-4 source is present, but the current native qualification entrypoint
still blocks Claude until its route-specific contract is integrated.

Build only the selected package, using a caller-owned shared target directory. The package/binary pairs are:

| Adapter | Cargo package | Binary | Descriptor template |
|---|---|---|---|
| Codex | `swarm-adapter-codex` | `swarm-codex-adapter` | `crates/swarm-adapter-codex/module-descriptor.template.json` |
| OpenCode | `swarm-adapter-opencode` | `swarm-adapter-opencode` | `crates/swarm-adapter-opencode/registration/descriptor.template.json` |
| Command | `swarm-adapter-command` | `swarm-adapter-command` | `crates/swarm-adapter-command/module-descriptor.template.json` (version 3; version 2 is retained separately) |
| Antigravity | `swarm-antigravity-adapter` | `swarm-antigravity` | `modules/antigravity-rust/module-descriptor.template.json` (version 4) |
| Claude | `swarm-adapter-claude` | `swarm-adapter-claude` | `modules/claude-rust/module-descriptor.template.json` |

```powershell
$Manifest = 'C:\src\eliot-swarm-controller\crates\swarm-adapter-command\Cargo.toml'
$Package = 'swarm-adapter-command'
$Target = 'D:\build-cache\eliot-shared-target'
cargo build --manifest-path $Manifest --package $Package --release --locked --target-dir $Target
```

Use the resulting `$Target/release/<binary>.exe` as the installer source. Build the owner helper separately from `crates/swarm-process/Cargo.toml` (`--package swarm-process --bin swarm-module-owner`) and keep its installed absolute path and lowercase SHA-256 for local host config.

The host writes the private scoped launch plan and resolver map below its storage data directory after durable module demand. It invokes the pinned helper as `swarm-module-owner <absolute-plan-path> <absolute-resolver-map-path>`. The helper accepts exactly these two paths; its per-launch files are produced by the host.

Copy the selected descriptor template outside the checkout. Set `enabled` to `true` only when ready to make that version selectable, and replace the credential placeholder (`<INSTALLER_CREDENTIAL_FILE_REF>` or the retained version-2 `REPLACE_AT_INSTALL` token) with an opaque protected-reference name. Keep adapter identity, protocol, schema, command/event schemas, capabilities, lifecycle and restart policy aligned with that adapter's template. Do not put credential bytes or provider tokens in the descriptor. The installer fills `launch.executable` and `launch.executable_sha256` from the built file.

For Command version 3, keep the typed `module_host_config_path` argument marker unchanged; the supervisor materializes that schema-v1 IPC config inside the exact binding's private state directory at launch. Replace only the final `--config` argument with the absolute path to the operator-maintained Command native config JSON, and make sure that file exists and is readable by the adapter. The installer rejects unresolved `<INSTALLER_...>` and `REPLACE_AT_INSTALL` placeholders, preserves the typed marker, and copies neither the native config nor the Command CLI/mod. The native config contains the native executable and preserved mod paths; it does not select the model. Version 2 keeps its original descriptor and config contract for existing installations.

For Antigravity, use descriptor version 4 with the exact command schemas
`swarm.normalized_result_context@1`, `swarm.runtime_command@1`, and
`swarm.task_dispatch_context@1`, plus the exact event schemas
`swarm.normalized_result_page@1`, `swarm.runtime_outcome@1`, and
`swarm.task_dispatch_admission@1`. The `agent.result` capability admits the
typed status/provenance path; it does not qualify productive assistant-body
readback. The current native stream has no request, item, assistant-message,
or turn parent, so a normalized result that reaches the adapter is answered
with `RESULT_BODY_UNAVAILABLE`, while `antigravity_status` remains the bounded
Store-retained status page.

Use an existing dedicated absolute install root outside the controller/Codex/OpenCode trees. The root must already exist, remain within the installer's path limit, and contain no reparse-point traversal. Preview first, then repeat without `-WhatIf`:

```powershell
$Source = 'D:\build-cache\eliot-shared-target\release\swarm-adapter-command.exe'
$Descriptor = 'D:\release-input\command-descriptor.json'
$InstallRoot = 'D:\eliot\installed-modules'
$Installer = 'C:\src\eliot-swarm-controller\tools\modules\Install-ModuleArtifact.ps1'
pwsh -NoProfile -File $Installer -SourceExecutable $Source -DescriptorTemplate $Descriptor -InstallRoot $InstallRoot -WhatIf
# After reviewing the preview, run the same command without -WhatIf.
```

The installer creates a coordinate-hashed directory with `module.exe`, `module-descriptor.json`, `install-receipt.json`, and local `install-coordinate.json`. It never registers or launches anything and never replaces different bytes at an existing identity. Use a new artifact version for changed executable or descriptor bytes. The Command native config is operator-managed outside that immutable artifact receipt; keep its configured path available and protected from unintended edits. At host startup, the optional supervisor loads only configured descriptor files, checks the descriptor/receipt/executable hashes, then registers through its reserved local identity. Managers do not receive that credential. A descriptor template being installed is not a registration receipt; registration is also not a route selection.

## Package independent frontends

The independent package/binary coordinates are `swarm-mcp`/`swarm-mcp`,
`swarm-cli`/`swarm` and `swarm-gateway`/`swarm-gateway`. The separate controller
package provides `swarm-host`. Gateway is optional;
installing the base host does not require it.

`Build-SwarmFrontendProvenance.ps1` builds one selected release binary using an
existing external shared target and writes a new package directory. It requires
a clean committed checkout and records the artifact's own source, lockfile,
dependency and executable hashes. For example:

```powershell
$TargetDir = 'C:\Users\kleym\AppData\Local\Eliot\build\rust-env-target'
$PackageDirectory = 'D:\eliot\packages\swarm-mcp-new-version'
pwsh -NoProfile -File tools/ci/Build-SwarmFrontendProvenance.ps1 `
  -Package swarm-mcp -TargetDir $TargetDir -OutputDir $PackageDirectory
pwsh -NoProfile -File tools/modules/Install-SwarmFrontend.ps1 `
  -PackageDirectory $PackageDirectory -InstallDirectory 'D:\eliot\frontend-version' -WhatIf
```

Use a new output directory and an existing dedicated absolute install directory.
After reviewing the preview, the same installer command without `-WhatIf` stages
and installs the verified package. Existing differing bytes are not replaced.
Each artifact retains its own provenance; installed siblings are checked against
the shared IPC protocol, target triple and supported launcher arguments, not an
unrelated artifact's repository SHA. Packaging and installation do not start a
service or establish native qualification.

The frontend dependency manifest records a workspace-resolved Cargo graph. Its
package closure is traversed from the selected package through that graph, while
feature arrays are explicitly labeled workspace-unified; they are not claimed
to be the selected artifact's exact compiled feature set.

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
| Antigravity | `antigravity` | `eliot-antigravity.rust-headless.1` (select version 4) | `workspaceRoot` | `modelId = 'gemini-3.8-flash-high'`, `workspaceRoot`; optional `reasoningEffort`, `agent`, `dangerouslySkipPermissions` |

The standalone Rust Codex source descriptor template declares version `4` under the stable artifact ID `codex-rust-controller.1`. Keep that ID in the route; choose version `4` as the exact catalog coordinate in `module.route.select`. Version 4 retains the normalized dispatch contract and opts new bindings into normalized result pages with `swarm.normalized_result_context@1` and `swarm.normalized_result_page@1`. The result selector is exactly `{ "kind": "codex_assistant_result", "input_operation_id": "<exact task.dispatch operation ID>" }`; Store validates and seals that producer identity. Older selected descriptor versions and existing bindings are not upgraded by this selection. The generic installer copies the supplied descriptor version without translating or enabling it.

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

Set `enabled` only for routes you want to admit new work on; omitted route `enabled` defaults to `false`. Command v3's typed host-config marker resolves to a supervisor-generated per-binding IPC config; its final `--config` argument names the separate operator-maintained native Command JSON file. The installer copies neither the native config nor the native executable/mod. Use the Command adapter README for the exact JSON fields. Existing native services/endpoints, access, models, and provider credentials remain operator-managed and must be independently qualified.

After restarting with the local config, read `module.catalog.get`, note `catalog_revision`, and select the exact enabled descriptor for your authenticated Manager identity (Operator identity is also accepted):

```json
{
  "route_alias": "command-local",
  "module_id": "runtime.command",
  "artifact_id": "eliot-command.rust-headless.1",
  "version": "3",
  "expected_catalog_revision": 12
}
```

Call `module.route.select` with that object. Use the exact values and revision returned by the catalog; a stale revision must be reread. Selection applies only to that identity's future bindings. It does not change existing bindings or start a worker. A later admitted pending Operation creates module demand; the host provisions and verifies the binding-scoped IPC credential, then launches the adapter under the descriptor and owner-helper checks. `external_attach` on Codex/OpenCode does not grant control of their native service. `owned_service` on Command/Antigravity describes the adapter module lifecycle; it does not install or qualify their native CLI.

Command descriptor version 3 adds only the bounded `agent.result` status-page capability. Its page reports retained Operation facts with `native_response_identity: "unavailable"`, `execution_complete: false`, and `task_completion: "unknown"`; it does not contain inferred assistant text or authorize Task acceptance. Existing version-2 registrations retain their four-capability claim and are not upgraded by selecting version 3 for a future binding.

Antigravity descriptor version 4 adds the normalized-result schema pair to
the existing status-page contract. Its normalized path validates the exact
Store-sealed origin and then reports `RESULT_BODY_UNAVAILABLE` because the
native event cannot prove that response text belongs to the admitted input.
Its status page remains a status/diagnostic readback with
`native_response_identity: "unavailable"`, `execution_complete: false`, and
`task_completion: "unknown"`; version 4 is not a productive readback
qualification.

## Package executable workspace coordinates

`tools/ci/module-package-policy.json` pins the package, manifest, and single
binary target for each module-role build. The builder compares this table with
Cargo metadata and invokes only the declared `--bin`; a package with an
unexpected or additional binary target requires an explicit policy update before
it can be packaged. The builder records local source hashes, registry pins,
lockfile/toolchain and executable digests. Cargo's full-workspace resolved graph
can include workspace-unified edges/features, so its provenance manifest labels
that scope rather than presenting the feature list as the selected binary's
exact compiled feature set.

| Cargo package | Manifest | Binary target | Role |
|---|---|---|---|
| `swarm-adapter-codex` | `crates/swarm-adapter-codex/Cargo.toml` | `swarm-codex-adapter` | Codex adapter |
| `swarm-adapter-command` | `crates/swarm-adapter-command/Cargo.toml` | `swarm-adapter-command` | Command adapter |
| `swarm-antigravity-adapter` | `modules/antigravity-rust/Cargo.toml` | `swarm-antigravity` | Antigravity adapter |
| `swarm-adapter-opencode` | `crates/swarm-adapter-opencode/Cargo.toml` | `swarm-adapter-opencode` | OpenCode adapter |
| `swarm-process` | `crates/swarm-process/Cargo.toml` | `swarm-module-owner` | Per-module owner helper |
| `swarm-script-worker` | `crates/swarm-script-worker/Cargo.toml` | `swarm-script-worker` | Script executor |
| `swarm-observer` | `crates/swarm-observer/Cargo.toml` | `swarm-observer` | Optional observer CLI |
| `swarm-checks` | `crates/swarm-checks/Cargo.toml` | `swarm-checks` | Optional checks executor |
| `swarm-forge-worker` | `crates/swarm-forge-worker/Cargo.toml` | `swarm-forge-worker` | Forge worker |
| `swarm-bus` | `crates/swarm-bus/Cargo.toml` | `swarm-bus-dispatcher` | Managed bus dispatcher |

For example, this recipe packages just the bus dispatcher into a new output
directory, while sharing the caller-owned external Cargo target with other
sequential package builds:

```powershell
just package-module swarm-bus release `
  'D:\build-cache\eliot-shared-target' 'D:\artifacts\swarm-bus-release'
```

The root host is its own coordinate: `eliot-swarm-controller` / binary `swarm-host`,
built in release profile by `Build-SwarmHostProvenance.ps1` or
`just package-host`. The manual workflow offers the twelve module coordinates
above, that host, and the three independent frontends. It selects one package
per run, creates one external shared target and fresh package output, and keeps
host/frontend builds release-only. Gateway is optional for the base host.

## Build and install the standalone automation worker

`swarm-automation` / `swarm-automation-worker` is an explicit release package
coordinate. Build the host and worker as separate packages with separate fresh
output directories; each `build-manifest.json` retains its own source, target,
dependency, and image provenance. Do not copy the host source or image digest
into the worker record.

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

Review the worker coordinate, source commit/tree, `source_sha256`,
`image_sha256`, and host image digest, then repeat without `-WhatIf`. The
installer accepts only the positive worker coordinate and its exact release
manifest row, rehashes the package, staged bytes, and final sibling, and uses
create-only placement beside the selected host. Identical bytes are an
idempotent no-op; different bytes are rejected. It does not edit configuration,
write a registration ledger, restart, or launch either process. The returned
`image_sha256` is the worker pin for deployment evidence; host configuration
remains an explicit operator action.

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

Pass that absolute executable path and the chosen host executable to the common
installer:
`tools/modules/Install-StandaloneWorker.ps1 -WorkerCoordinate swarm-forge-worker
-HostExecutable <absolute-swarm-host.exe> -PackageExecutable <absolute-worker.exe>`.
`-WhatIf` previews placement. The installer validates the exact package
manifest, clean source revision, binary target, length, own source digest, and
image SHA-256, then places the worker beside that host. Identical installed
bytes are a no-op; different bytes are not overwritten. It does not configure
or launch either process. The unsigned build manifest provides consistency
evidence; it is not a signature. The existing specialized
`Install-ForgeWorker.ps1` remains compatible for operators who need that
narrow coordinate-specific entrypoint.

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

## Build and install the managed bus dispatcher

Build package `swarm-bus` with the existing module package builder, using the
same external shared Cargo target directory and a new explicit output directory.
Pass its `bin/swarm-bus-dispatcher.exe` to
`tools/modules/Install-BusDispatcher.ps1 -PackageExecutable <absolute-dispatcher.exe>
-InstallDirectory <existing-absolute-directory>`. Use `-WhatIf` to preview.
The installer verifies the exact release package, clean source manifest,
dependency graph scope, binary target, length and the dispatcher's own image
SHA-256. Identical installed bytes are a no-op; different bytes are rejected.
It does not edit configuration, create a registration record, restart, or
launch the dispatcher. The returned `sha256` is the pin for the selected
dispatcher artifact; it is not compared with an unrelated host or worker
artifact.

Configure the returned values explicitly:

```toml
[bus_supervisor]
enabled = true
dispatcher_executable = 'C:\ELIOT\artifacts\swarm-bus-dispatcher.exe'
dispatcher_sha256 = '<64-lowercase-hex-digest-returned-by-installer>'
```

The supervisor starts this image only for an explicitly managed, ready Store
registration and keeps the dispatcher lifecycle isolated. Installing the
binary and setting its pin do not create that registration or qualify a live
bus service.

## Package the host with source provenance

`tools/ci/Build-SwarmHostProvenance.ps1 -TargetDir <existing-shared-target>
-OutputDir <new-package-directory>` manually builds only the release host package
through the existing builder. It emits `bin/swarm-host.exe` and the source/build/image
manifest used to bind a qualification run to an exact artifact. Normal source
pushes do not invoke this release entrypoint. The caller supplies the existing
shared target directory; no separate worker or worktree build cache is required.

## Optional checks and local observer

The manual `module-package.yml` workflow offers 14 current executable package
coordinates: ten module roles, the root host, and three standalone frontends.
Select one package and profile; host and frontends require `release`. All choices
use one runner shared target directory and produce one package manifest.
Library-only scheduler/supervisor crates are not executable selections.
For current-source native qualification, use
`tools/qualification/New-NativeQualification.ps1` with separate explicit
`-HostExecutable`, `-ExpectedHostSha256`, `-HostBuildManifestPath`,
`-ExpectedHostBuildManifestSha256` and `-PublicCliExecutable`,
`-ExpectedPublicCliSha256`, `-PublicCliBuildManifestPath`,
`-ExpectedPublicCliBuildManifestSha256` arguments, plus the installed module
descriptor, module manifest, and private configuration inputs. The host image
is `swarm-host.exe`; the public CLI is `swarm.exe` and is staged beside its
required host sibling. Their manifests and image hashes are independently
pinned; the CLI and host may have different source revisions. The installed
module still has to match the host's retained build-set contract. The harness
starts the host directly, uses the public CLI for ordinary IPC, creates a fresh
DataRoot, protects unrelated/current Codex processes, and submits one native
input. Unknown effects use bounded readback rather than another send.
Source/manifest consistency is recorded separately from the runtime result.

`tools/qualification/Invoke-CoreFailureQualification.ps1` uses an explicitly
pinned `-HostExecutable` and `-PublicCliExecutable` with their independent image
hashes and build manifests. It starts only the host image directly; the public
CLI handles Manager operations, ordinary IPC, and independent readback. It uses
a fresh private DataRoot for lost-caller-ACK, request-conflict and optional
HookSource deduplication checks. Readback must prove admission before graceful
restart. The harness never treats a missing caller reply as a native-effect
unknown outcome, injects a vendor effect, or stops another host. Source
availability and AST validation do not establish a passing run.

The checks executor is a separate optional process pin, not a module descriptor. Build package `swarm-checks` and configure its binary using `[checks.executor]`. Checks default disabled, and the executor pin defaults absent:

```toml
[checks.executor]
executable = 'D:\eliot\bin\swarm-checks.exe'
sha256 = '<64-lowercase-hex-sha256-of-that-file>'
artifact_id = 'swarm-checks'
version = '0.1.0'
```

The host verifies the executable path and image digest, including before Store Go. `artifact_id` and `version` are retained evidence labels; they are not read from binary metadata. A bad path or digest fails that launch without switching to the legacy worker.

The host `[observability]` recorder defaults disabled. Its current fields are
`enabled`, optional `directory` and `live_config_file`, `queue_records`,
`queue_bytes`, `max_record_bytes`, `file_segment_bytes`, `retention_bytes`, and
`retention_days`; omitted values use the current config defaults. Relative paths
resolve beside the controller config, while the default recording directory is
`<storage.data_dir>/diagnostics`. The optional pinned JSON live-config schema 3
sets level, content mode, included diagnostic kinds, exact one-selector overrides,
and retention when the lazy recorder runs; schemas 1 and 2 remain metadata-only.
Metadata is the default. Only explicitly selected bounded module-supervisor
lifecycle text may be Atlas-redacted and captured. Raw prompts, tool arguments,
environment values, authorization headers, credentials, and native frames are
not captured.

The standalone `swarm-observer` binary takes an absolute private directory and optional `--queue-records`, `--queue-bytes`, `--max-record-bytes`, `--segment-bytes`, `--retention-bytes`, and `--retention-days`; it reads newline-delimited diagnostic records from stdin.
