# PR25 supervisor package contract

This package closes the manual packaging seam for the optional supervisor actor and its per-scope process owner helper. The supervisor has no separate executable: `swarm-supervisor` is a library linked into the host. The configured host executable is the root package's `swarm` binary (`eliot-swarm-controller`, `src/main.rs`). The standalone `swarm-module-owner.exe` target belongs to `swarm-process` (`crates/swarm-process/src/bin/swarm-module-owner.rs`). It is neither a module descriptor artifact nor a native-agent launcher.

The bin-only `tools/ci/build-module-package.ps1` must not approve library packages as module packages. This overlay carries `module-package-policy.patch` to remove library-only `swarm-supervisor` from the allowlist; `swarm-observer` remains approved because it has the auto-discovered `src/bin/swarm-observer.rs` target. `swarm-process` remains approved and produces the actual helper target. The package script resolves the exact workspace member and target from Cargo metadata, invokes `cargo build --package <package> --bins`, copies each bin to `OutputDir/bin`, and records package/target/hash evidence in `build-manifest.json`. The root host `swarm.exe` is built and distributed by the host release path; it is intentionally not a module package.

## Manual helper packaging

1. From a clean, committed source checkout, package only the process crate into caller-owned directories. This command is a future operator recipe; it was not run for this source slice:

   ```powershell
   pwsh -NoProfile -File .\tools\ci\build-module-package.ps1 `
     -Package swarm-process -Profile release `
     -TargetDir D:\build-cache\eliot-swarm-target `
     -OutputDir D:\release\swarm-process
   ```

   The expected helper is `D:\release\swarm-process\bin\swarm-module-owner.exe`; the adjacent `build-manifest.json` must name package `swarm-process`, bin `swarm-module-owner`, and matching artifact SHA-256. The package script itself checks a clean checkout before and after its Cargo build. `swarm-supervisor` is not passed to it because that crate is a library and has no bin target.

2. Point the copy-only entry at the actual configured `swarm.exe` host path and that exact packaged helper:

   ```powershell
   $installed = .\tools\modules\Install-ModuleOwnerHelper.ps1 `
     -HostExecutable 'C:\Program Files\Eliot Swarm\swarm.exe' `
     -PackageExecutable 'D:\release\swarm-process\bin\swarm-module-owner.exe' `
     -WhatIf
   $installed
   ```

   Review the resolved target and digest, then invoke the same command without `-WhatIf`. The installer accepts only the named helper from a matching package manifest, rejects reparse traversal, copies through a flushed same-directory staging file, rehashes the staged and installed bytes, and never overwrites a different helper. It writes no config, descriptor, Store state, or secret. Existing identical bytes are an idempotent no-op. The script never launches either executable.

3. Copy the returned absolute `owner_helper` and lowercase `owner_helper_sha256` values into the host config's `[module_supervisor]` table. The exact field names and minimum table shape are in [`module-supervisor.example.toml`](module-supervisor.example.toml). The actor checks the helper's actual bytes against the configured SHA-256 before accepting it. The host does not discover the helper from `current_exe()`: set the explicit path returned by the installer. The enabled actor also needs an absolute `install_root`, at least one exact installed `descriptor_files` entry, and any additional protected-ref file mappings declared by those descriptors; the host supplies its reserved supervisor credential internally.

The current `Config::load` bridge resolves relative `module_supervisor` paths against the parent directory of `--config` (or the current directory when no config file was supplied), not against the host executable's directory. Using the installer's absolute path avoids that ambiguity. With a config file in the same directory as the host and helper, a relative `owner_helper = "swarm-module-owner.exe"` can resolve there; the installed example deliberately uses the absolute value.

## Module descriptors remain separate

Use the existing `tools/modules/Install-ModuleArtifact.ps1` only for already-built adapter/module executables plus their versioned `ModuleDescriptor` template. It installs under the explicit module `install_root`, creates `module.exe`, `module-descriptor.json`, and `install-receipt.json`, and does not install helper binaries or configure the host. The supervisor's `descriptor_files` is a list of the exact `descriptor_file` paths returned by those installer invocations; `install_root` is their common module installation root. Replace the all-zero sample path in [`module-supervisor.example.toml`](module-supervisor.example.toml) with the actual installed descriptor path. A descriptor/receipt pair is local install evidence, not executable activation or a trust root; the supervisor independently rehashes and validates it before the reserved registration RPC.

The module-owner helper is selected only by host config and is launched as an isolated per-scope bootstrap. Copying it beside the host does not grant it authority to start or terminate an external native service. For descriptors whose `lifecycle` is `external_attach`, only the adapter client process is owned; the existing native service remains untouched. Do not add the helper itself to `descriptor_files`, do not make a descriptor for the helper, and do not put credentials in its arguments or the config snippet.

## Source checks and limits

- The package mapping above was read from current Cargo manifests and discovered source targets. `swarm-supervisor` has no bin target; `swarm-observer` has the auto-discovered `src/bin/swarm-observer.rs`; `swarm-process` explicitly declares `swarm-module-owner`; the root package explicitly declares `swarm`.
- The installer source received a PowerShell AST syntax parse only; it was not executed. It uses the packaging script's unsigned `build-manifest.json` as consistency evidence, then independently rehashes the binary at copy time. The manifest is not a signature or trust root; release artifact custody still depends on the operator's source/release process and host-directory ACL.
- No tracked file, config, descriptor, or install was written. No Cargo build/test, runtime, process, Store, native service, or Git command was run for this package.
