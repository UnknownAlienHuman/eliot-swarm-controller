# Install and select a local module

This runbook covers the standalone adapter packages, independent frontend binaries, and local observer/check executor settings in the current controller config. It installs adapter executables only. It does not install a provider, native service, model, or vendor CLI, and installation or route selection does not qualify those native systems.

### Build and install one adapter

Model selection in qualification and production routes is configuration-driven: pass the operator-configured model ID (`-OpenCodeCommandTestModelRef`, `-CommandTestModelId`, `-ClaudeTestModelId`, or the route's `native_options.model` / `modelId`) that matches a model available in the target provider's catalog. Before creating a native session, `agent.open` validates that the configured route and model are supported by the active provider catalog; if absent, `agent.open` fails cleanly before `task.dispatch`. Do not hardcode specific promotional or deprecated model names into adapter code or validation gates.

Build only the selected package, using a caller-owned shared target directory. The package/binary pairs are:

| Adapter | Cargo package | Binary | Descriptor template | Protocol / Donor basis |
|---|---|---|---|---|
| Codex | `swarm-adapter-codex` | `swarm-codex-adapter` | `crates/swarm-adapter-codex/module-descriptor.template.json` (version 11.4) | Codex `app-server` JSON-RPC / `openai-codex` SDK bridge |
| OpenCode | `swarm-adapter-opencode` | `swarm-adapter-opencode` | `crates/swarm-adapter-opencode/registration/descriptor.template.json` (`0.7.0`, `opencode-v2-native-options@4`) | `@opencode/server` / `@muse-code/sdk` HTTP+SSE |
| Command | `swarm-adapter-command` | `swarm-adapter-command` | `crates/swarm-adapter-command/module-descriptor.template.json` (Batch v5); `module-descriptor-acp.template.json` (ACP v2) | Headless CLI structured NDJSON / `agent-client-protocol` (ACP) |
| Antigravity | `swarm-antigravity-adapter` | `swarm-antigravity` | `modules/antigravity-rust/module-descriptor.template.json` (version 6); Node `modules/antigravity/module-descriptor.template.json` (`antigravity-cli-warm-bridge.4`) | ACP (`agent-client-protocol`) / headless turn stream |
| Claude | `swarm-adapter-claude` | `swarm-adapter-claude` | `modules/claude-rust/module-descriptor.template.json` | `@anthropic-ai/claude-agent-sdk` / Claude `stream-json` |

```powershell
$Manifest = 'C:\src\eliot-swarm-controller\crates\swarm-adapter-command\Cargo.toml'
$Package = 'swarm-adapter-command'
$Target = 'D:\build-cache\eliot-shared-target'
cargo build --manifest-path $Manifest --package $Package --release --locked --target-dir $Target
```

Use the resulting `$Target/release/<binary>.exe` as the installer source. Build the owner helper separately from `crates/swarm-process/Cargo.toml` (`--package swarm-process --bin swarm-module-owner`) and keep its installed absolute path and lowercase SHA-256 for local host config.

The host writes the private scoped launch plan and resolver map below its storage data directory after durable module demand. It invokes the pinned helper as `swarm-module-owner <absolute-plan-path> <absolute-resolver-map-path>`. The helper accepts exactly these two paths; its per-launch files are produced by the host.

Copy the selected descriptor template outside the checkout. Set `enabled` to `true` only when ready to make that version selectable, and replace the credential placeholder (`<INSTALLER_CREDENTIAL_FILE_REF>` or the retained version-2 `REPLACE_AT_INSTALL` token) with an opaque protected-reference name. Keep adapter identity, protocol, schema, command/event schemas, capabilities, lifecycle and restart policy aligned with that adapter's template. Do not put credential bytes or provider tokens in the descriptor. The installer fills `launch.executable` and `launch.executable_sha256` from the built file.

For current Command Batch version 5 and ACP version 2, keep the typed `module_host_config_path` argument marker unchanged; the supervisor materializes that schema-v1 IPC config inside the exact binding's private state directory at launch. Replace only the final `--config` argument with the absolute path to the operator-maintained Command native config JSON, and make sure that file exists and is readable by the adapter. The installer rejects unresolved `<INSTALLER_...>` and `REPLACE_AT_INSTALL` placeholders, preserves the typed marker, and copies neither the native config nor the Command CLI/mod. The native config contains the native executable and preserved mod paths; it does not select the model. Current profiles permit an omitted route model and inherit its native default. Retained Batch versions 2/3/4 and ACP version 1 keep their original descriptor, config and receipt contracts for existing bindings.

For Antigravity (Rust descriptor version 6 / v4+ and Node `antigravity-cli-warm-bridge.4`) and OpenCode, the descriptor advertises command schemas `swarm.normalized_result_context@1`, `swarm.runtime_command@1`, and `swarm.task_dispatch_context@1`, plus event schemas `swarm.normalized_result_page@1`, `swarm.runtime_outcome@1`, and `swarm.task_dispatch_admission@1`. Because their native streams are session/turn-scoped rather than attaching a parent-input ID to every chunk, the adapter correlates assistant output using the **serialized active turn / busy window** `(binding_id, binding_generation, native_session_id, active_turn_id)` sealed to the admitted `task.dispatch` or `agent.send` Operation (Waku/Paseo turn-correlation pattern) and returns the assembled assistant response through `swarm.normalized_result_page@1`. `RESULT_BODY_UNAVAILABLE` is returned only when a native stream crashes or truncates before emitting terminal assistant output.

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

Each route uses the Rust artifact ID and workspace field enforced by `src/config.rs`:

| Route | `runtime` | `module_artifact_id` | `workspace_option` | `native_options` |
|---|---|---|---|---|
| Codex | `codex` | `codex-rust-controller.1` | `workspaceRoot` | `workspaceRoot`; optional explicit `modelProvider` and `model` |
| OpenCode | `module` | `eliot-opencode-v2.rust-http.1` | `directory` | `service_id`, `connection_file`, `directory`; optional explicit `model = { id, providerID, variant }` |
| Command | `command` | `eliot-command.rust-headless.1` | `workspaceRoot` | `workspaceRoot`; optional explicit `modelId` |
| Antigravity | `antigravity` | `eliot-antigravity.rust-headless.1` (version 6) | `workspaceRoot` | `workspaceRoot`; optional explicit `modelId`, `reasoningEffort`, `agent`, `dangerouslySkipPermissions` |
| Claude | `module` | `eliot-claude.rust-headless.1` | `workspaceRoot` | `workspaceRoot`; optional explicit `modelId` |

The standalone OpenCode coordinate `eliot-opencode-v2.rust-http.1@0.7.0` uses the `opencode-v2-native-options@4` schema in its descriptor. Its external-attach route has no mandatory `expected_version` pin; observed server version remains diagnostic and compatible server versions are accepted by protocol capability. Native model defaults apply when no override is supplied. The built-in `eliot-opencode-v2.http.2` route remains supported for TaskPrompt bindings; legacy `.1` bindings remain readable for historical Operations.

The standalone Rust Codex descriptor template declares version `11.4` under the stable artifact ID `codex-rust-controller.1`. Choose version `11.4` as the catalog coordinate in `module.route.select`. Version 4+ uses the normalized dispatch contract and normalized result pages (`swarm.normalized_result_context@1` and `swarm.normalized_result_page@1`), and version 8+ derives the local native-store namespace from canonical `codexHome`. The result selector is `{ "kind": "codex_assistant_result", "input_operation_id": "<exact task.dispatch operation ID>" }`; Store validates and seals that producer identity.

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

Set `enabled` only for routes you want to admit new work on; omitted route `enabled` defaults to `false`. Command Batch v5 / ACP v2's typed host-config marker resolves to a supervisor-generated per-binding IPC config; its final `--config` argument names the separate operator-maintained native Command JSON file. The installer copies neither the native config nor the native executable/mod. Use the Command adapter README for the exact JSON fields. Existing native services/endpoints, access, models, and provider credentials remain operator-managed and must be independently qualified.

After restarting with the local config, read `module.catalog.get`, note `catalog_revision`, and select the exact enabled descriptor for your authenticated Manager identity (Operator identity is also accepted):

```json
{
  "route_alias": "command-local",
  "module_id": "runtime.command",
  "artifact_id": "eliot-command.rust-headless.1",
  "version": "5",
  "expected_catalog_revision": 12
}
```

Call `module.route.select` with that object. Use the exact values and revision returned by the catalog; a stale revision must be reread. Selection applies only to that identity's future bindings. It does not change existing bindings or start a worker. A later admitted pending Operation creates module demand; the host provisions and verifies the binding-scoped IPC credential, then launches the adapter under the descriptor and owner-helper checks. `external_attach` on Codex/OpenCode does not grant control of their native service. `owned_service` on Command/Antigravity describes the adapter module lifecycle; it does not install or qualify their native CLI.

Command descriptor version 3+ (Batch v5 / ACP v2) adds `agent.result` with the normalized result context/page schemas. `command_status` returns retained Operation facts. `command_output` returns bounded pages from the exact journaled `stdout.ndjson` or `stderr.txt` capture sealed to the admitted dispatch and byte digest; incomplete or unreadable captures are not accepted as complete results. Complete pages can be assembled and submitted through the existing candidate path, where the Manager retains acceptance authority.

Antigravity (descriptor version 4+ / v6) and OpenCode support productive `agent.result` readback with the normalized-result schema pair (`swarm.normalized_result_context@1` and `swarm.normalized_result_page@1`). Their normalized path validates the Store-sealed dispatch origin, correlates the assistant response across the active turn's serialized busy window on `(binding_id, binding_generation, native_session_id, active_turn_id)`, and returns bounded pages of the assistant output. `RESULT_BODY_UNAVAILABLE` is returned only when the native stream terminated or errored before producing assistant output.

## Hot descriptor installation (Operator-only, no live restart)

Method `module.descriptor.install` (Mutation, Operator-only with `require_local_operator`, single-flight `MAX_PENDING_DESCRIPTOR_INSTALLS = 1`) admits an already-installed descriptor file (`ModuleDescriptorInstallRequest { descriptor_file: absolute PathBuf }`) into the live catalog without restarting the host or active workers:
- **Installed receipt & hash verification**: the file must exist under the configured absolute `install_root` alongside its `install-receipt.json` (`eliot.module_install_receipt.v1`). Both `load_installed_descriptor` and supervisor registration (`register_installed_descriptor`) verify canonical paths, descriptor schema, and normalized SHA-256 equality across descriptor, receipt, and executable bytes (`MODULE_INSTALL_RECEIPT_MISMATCH` / `MODULE_ARTIFACT_MISMATCH`).
- **Non-disruptive catalog extension**: a successful install extends the descriptor catalog (`extend_descriptor_catalog`, bumping `catalog_revision`), verifies that the combined `SupervisorBootstrap` (max 512 retained descriptor paths) fits within the 65,536-byte child bootstrap frame (`MODULE_BOOTSTRAP_LIMIT`), and appends the validated path for future supervisor restarts (`append_retained_descriptor_files`). Active bindings and worker PIDs continue running undisturbed; managers can select the newly registered version for future bindings via `module.catalog.get` and `module.route.select`.

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

The host chain has three separate coordinates: the public `swarm-host` wrapper
in `eliot-swarm-controller`, the actual `swarm-kernel-host`, and the independent
`swarm-supervisor`. `Build-SwarmHostProvenance.ps1` records the selected host
coordinate; frontend manifests declare all required sibling coordinates. The
manual workflow offers the declared module, host and frontend packages. It
selects one package per run, uses one external shared target and a fresh package
output, and keeps host/frontend builds release-only. Gateway is optional for
the base host.

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

## Package runtime processes with source provenance

Use `Build-SwarmHostProvenance.ps1` for the public `swarm-host` launcher,
`Build-SwarmKernelHostProvenance.ps1` for `swarm-kernel-host`, and
`Build-SwarmSupervisorProvenance.ps1` for `swarm-supervisor`. Each accepts the
existing shared `-TargetDir` and a fresh `-OutputDir`; each emits its own
binary and source/build/image manifest. The public launcher forwards arguments
to the Kernel sibling. The Kernel owns Store, the database lock and IPC.
Normal source pushes do not invoke these manual release builders.

## Optional checks and local observer

The manual `module-package.yml` workflow offers explicit executable package
coordinates, including module roles, the public launcher, Kernel, supervisor
and standalone frontends. Select one package and profile; runtime processes and
frontends require `release`. All choices use one shared target and produce one
package manifest.

Qualification harnesses (`tools/qualification/New-NativeQualification.ps1` and `tools/qualification/Invoke-CoreFailureQualification.ps1`) verify the four core runtime binaries (`swarm-kernel-host.exe`, `swarm-host.exe`, `swarm-supervisor.exe`, and `swarm.exe`) and their `build-manifest.json` receipts. To avoid brittle 16-argument command lines and manual hash copy-paste errors, the harnesses should read image and manifest SHA-256 digests directly from the staged release directory's `build-manifest.json` / `install-receipt.json` files (while still accepting explicit override arguments for compatibility). Normalize all SHA-256 strings to 64 lowercase hexadecimal characters (stripping any optional `sha256:` prefix) and parse manifest JSON with forward-compatible readers that ignore additive optional metadata fields.

The binaries are staged together with the declared sibling chain and Kernel resources. Their manifests and image hashes are independent; source revisions may differ if their declared IPC contracts agree. Readiness and recorded host PID/image refer to the Kernel. The harness owns that Kernel's stdin for graceful EOF shutdown, while ordinary requests use the public CLI.

Native qualification also takes the installed module descriptor, owner helper and private configuration inputs, creates a fresh DataRoot, protects unrelated existing processes, and runs a productive end-to-end turn (`agent.open -> task.dispatch -> agent.result`). Unknown effects use bounded readback. Source/manifest consistency and runtime outcome are recorded separately.

Core failure qualification uses a fresh private DataRoot for lost-caller-ACK, request-conflict and optional HookSource deduplication. Readback proves admission before graceful restart. Missing caller replies remain delivery uncertainty; this harness does not inject vendor effects or stop another host.

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
