# Module artifact installer

This directory contains a PowerShell 7 installer for an executable that
has already been built elsewhere. It does not build, inspect by execution,
launch, register, enable, route, or configure the module. It writes only under
the explicit absolute `InstallRoot` supplied by the operator; that root must
already exist and must have no reparse-point ancestor.

The descriptor template follows the frozen
`swarm_contracts::module_catalog::ModuleDescriptor` JSON shape. Start with
[`descriptor.template.example.json`](descriptor.template.example.json), then
replace its identity, protocol, schema, capability, launch, and restart values
with metadata for the exact artifact being installed. Keep protected launch
values as `{ "kind": "protected", "value": "opaque-reference" }`. Never put
credential bytes, API tokens, or secret literals in the template. The script
rejects secret-named environment literals and common secret-bearing argv
flags, and never resolves a protected reference.

Build only the selected module package, passing one caller-owned target root:

```powershell
$ModulePackageRoot = 'D:\src\selected-module'
$ModulePackageName = 'swarm-adapter-selected'
$SharedTargetRoot = 'D:\build-cache\eliot-shared-target'
cargo build --manifest-path (Join-Path $ModulePackageRoot 'Cargo.toml') `
  --package $ModulePackageName --release --locked --target-dir $SharedTargetRoot
```

Use the built executable from that explicit package's output directory as the
source. Do not run a workspace-wide default build, use a per-worker target
directory, or run `cargo clean`. The command above is a recipe only; this task
does not execute it.

Create a dedicated module install root explicitly, then preview the exact
result. The source executable and descriptor template must also be absolute:

```powershell
$Source = 'D:\build-cache\eliot-shared-target\release\selected-adapter.exe'
$Template = 'D:\release-input\module-descriptor.template.json'
$InstallRoot = 'D:\eliot\installed-modules'
pwsh -NoProfile -File .\Install-ModuleArtifact.ps1 `
  -SourceExecutable $Source -DescriptorTemplate $Template `
  -InstallRoot $InstallRoot -WhatIf
```

After reviewing the preview, invoke the same command without `-WhatIf`.

The destination uses one full SHA-256 scope token over compact ordered JSON for
the exact `(module_id, artifact_id, version)` tuple. Metadata identifiers never
become path components:

```text
<InstallRoot>/artifact-<sha256-of-coordinate>/
  module.exe
  install-receipt.json
  module-descriptor.json
  install-coordinate.json
```

At that same 26-character root, the former nested module/artifact/version
layout made `module-descriptor.json` 268 characters:
`26 + 1 + (7 + 64) + 1 + (9 + 64) + 1 + (8 + 64) + 1 + 22 = 268`.
That already exceeds the installer's 240-character limit before staging.

With the compact scope, the longest committed file path is 124 characters,
computed as
`26 + 1 + (9 + 64) + 1 + 23 = 124`:
`D:\eliot\installed-modules\artifact-<64-hex>\install-coordinate.json`.
The descriptor, receipt, and binary paths are 123, 121, and 111 characters.
The same-directory `.stage-<32-hex>` temporary file is 140 characters at this
root and is included in the same preflight check.
The 240-character containment check remains in force for longer operator-
supplied roots.

The installer copies to a random same-directory staging file, flushes and hashes
it, confirms source and staged bytes agree, then uses a no-overwrite atomic file
move. It hashes the finalized executable again before publishing metadata. A
full exact identity marker, binding the tuple, build ID, source path/hash,
descriptor hash, and receipt hash, is atomically published before the executable
copy. The marker is byte-compared on every retry, so even a full-SHA scope
collision or changed descriptor/source cannot adopt a partial destination. It
is not an install-completion marker. The receipt is atomically written first;
the descriptor is written last and remains the completion marker. A consumer
must require both files and verify their hashes.
The receipt is deliberately unsigned local install evidence, not an authority
grant or trust root. The supervisor must independently validate the descriptor,
receipt identity/path/digests and re-hash the executable before it calls the
existing `module.descriptor.register` RPC with only
`StoreOwner::module_supervisor_credential()`.

The receipt fields are exactly:

```json
{
  "schema_version": 1,
  "format": "eliot.module_install_receipt.v1",
  "module_id": "...",
  "artifact_id": "...",
  "version": "...",
  "build_id": null,
  "source_file": "absolute path",
  "installed_file": "absolute path",
  "source_sha256": "lowercase 64-hex",
  "staged_sha256": "lowercase 64-hex",
  "installed_sha256": "lowercase 64-hex",
  "descriptor_file": "absolute path",
  "descriptor_sha256": "lowercase 64-hex"
}
```

`build_id` is the descriptor's exact string value or `null`. The additional
`install-coordinate.json` file is local collision/retry evidence; the supervisor
loader ignores it and continues to validate the existing descriptor/receipt
pair.

All three executable digests are identical. `descriptor_sha256` covers the
exact UTF-8 descriptor file bytes including its final LF. The supervisor should
hash the file bytes directly. A partial install can be retried with the same
source and template; it will finish matching metadata without replacing a file.
Reusing a module/artifact/version coordinate for different executable or
descriptor bytes fails; publish a new artifact version instead. Older versions
remain in place for existing Windows processes and rollback. The compact layout
is used for new installations; previously installed nested-layout
artifacts are left untouched and keep their original descriptor paths.

The installer accepts a Windows `.exe` source, stores it under the stable name
`module.exe`, and refuses a source named exactly like the core swarm, Codex, or
OpenCode executable; rejects reparse points in source/root/destination paths;
requires every generated path to remain beneath `InstallRoot`; uses encoded
path tokens; and refuses known Codex/OpenCode/swarm root path components. It
never deletes any existing artifact. As with ordinary filesystem operations,
the install root's ACL must prevent an untrusted concurrent process from
replacing directories between checks; this is not a same-user sandbox.

## Handoff and limits

- `Install-ModuleArtifact.ps1` creates a versioned executable, one installed
  `ModuleDescriptor`, and the deterministic receipt. It never connects to the
  Store or supervisor.
- The supervisor's local catalog loader consumes the descriptor/receipt pair,
  checks their exact file hashes and identities, then uses its independent
  executable hash check and the dedicated registration credential. It does not
  inherit a Manager bearer credential.
- This installer does not enable a route, mutate config, start a process, load a
  model, or change Store state. `enabled` is copied from the supplied descriptor
  template and remains a separate catalog property.
- File staging and each metadata-file publication are atomic renames on the
  same filesystem. The descriptor-last marker makes recovery from a partial
  publication deterministic; the filesystem does not provide an atomic
  three-file transaction.
- The PowerShell source was not executed. No binary was installed, no module
  was registered, and no build, test, runtime, database, or configuration
  operation was performed.
