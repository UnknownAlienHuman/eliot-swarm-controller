[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
param(
    [Parameter(Mandatory = $true)] [ValidateNotNullOrEmpty()] [string] $PackageDirectory,
    [Parameter(Mandatory = $true)] [ValidateNotNullOrEmpty()] [string] $InstallDirectory,
    # Retained for caller compatibility only. New Antigravity-only packages
    # run no npm install, so these are accepted and ignored; historical
    # receipt readback never launches a toolchain either. Root owns the CI
    # workflow argument updates separately.
    [string] $NodeExecutable,
    [string] $NpmCliScript,
    [string] $ExpectedNodeVersion,
    [string] $ExpectedNpmVersion
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (-not $IsWindows) { throw 'This create-only installer accepts Windows frontend packages only.' }

$packageMap = @{
    'eliot-swarm-controller' = [ordered]@{
        manifest = 'Cargo.toml'
        binary = 'swarm-host'
        role = 'host_launcher'
        required_siblings = @('swarm-kernel-host')
    }
    'swarm-supervisor' = [ordered]@{
        manifest = 'crates/swarm-supervisor/Cargo.toml'
        binary = 'swarm-supervisor'
        role = 'host_supervisor'
        required_siblings = @()
    }
    'swarm-kernel-host' = [ordered]@{
        manifest = 'crates/swarm-kernel-host/Cargo.toml'
        binary = 'swarm-kernel-host'
        role = 'host_runtime'
        required_siblings = @('swarm-supervisor')
        # Retired: new kernel-host packages carry no OpenCode private
        # SDK/server resources and run no npm install. They carry only the
        # Antigravity Node bridge group below. The retired
        # swarm-kernel-host-opencode-resources coordinate (serve.mjs,
        # native-mcp-proof.mjs, index.mjs, package.json, package-lock.json
        # plus the locked npm/node_modules dependency closure) is accepted
        # only as fenced read-only inventory of historical manifests and
        # receipts (see Assert-ResourceManifest -AllowLegacyMissingAdditionalGroups
        # and Assert-DependencyClosure); it is never staged or installed anew.
        additional_resource_groups = @(
            [ordered]@{
                coordinate = 'swarm-kernel-host-antigravity-node-bridge'
                module_id = 'antigravity'
                artifact_id = 'antigravity-cli-warm-bridge.4'
                version = '4'
                repository_relative_root = 'modules/antigravity'
                installed_relative_root = 'resources/modules/antigravity'
                resource_files = @(
                    [ordered]@{ path = 'bridge.mjs'; role = 'retained_bridge_v3_entry' },
                    [ordered]@{ path = 'bridge-v4.mjs'; role = 'bridge_v4_entry' },
                    [ordered]@{ path = 'codec.mjs'; role = 'native_stream_codec' },
                    [ordered]@{ path = 'control.mjs'; role = 'authenticated_host_control_client' },
                    [ordered]@{ path = 'module-descriptor.template.json'; role = 'module_descriptor_template' },
                    [ordered]@{ path = 'package.json'; role = 'node_package_manifest' },
                    [ordered]@{ path = 'receipt-state.mjs'; role = 'local_receipt_state' }
                )
            }
        )
    }
    'swarm-mcp' = [ordered]@{
        manifest = 'crates/swarm-mcp/Cargo.toml'
        binary = 'swarm-mcp'
        role = 'frontend_service'
        required_siblings = @()
    }
    'swarm-gateway' = [ordered]@{
        manifest = 'crates/swarm-gateway/Cargo.toml'
        binary = 'swarm-gateway'
        role = 'frontend_service'
        required_siblings = @()
    }
    'swarm-cli' = [ordered]@{
        manifest = 'crates/swarm-cli/Cargo.toml'
        binary = 'swarm'
        role = 'cli_client'
        required_siblings = @('eliot-swarm-controller')
    }
}

function Test-HostPackage([string] $Name) {
    return $Name -in @('eliot-swarm-controller', 'swarm-kernel-host', 'swarm-supervisor')
}

function Get-CanonicalPath([string] $Path, [string] $Label) {
    if (-not [IO.Path]::IsPathFullyQualified($Path)) { throw "$Label must be an absolute path." }
    return [IO.Path]::GetFullPath($Path)
}

function Test-PathWithin([string] $Path, [string] $Root) {
    $pathFull = [IO.Path]::GetFullPath($Path)
    $rootFull = [IO.Path]::GetFullPath($Root).TrimEnd([char[]]@(
        [IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar
    ))
    if ($pathFull.Equals($rootFull, [StringComparison]::OrdinalIgnoreCase)) { return $true }
    return $pathFull.StartsWith($rootFull + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)
}

function Assert-NoReparseTraversal([string] $Path) {
    $fullPath = [IO.Path]::GetFullPath($Path)
    $root = [IO.Path]::GetPathRoot($fullPath)
    if ([string]::IsNullOrWhiteSpace($root)) { throw 'Path has no filesystem root.' }
    $cursor = $root
    $segments = $fullPath.Substring($root.Length).Split(
        [char[]]@([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar),
        [StringSplitOptions]::RemoveEmptyEntries
    )
    foreach ($segment in $segments) {
        $cursor = [IO.Path]::Combine($cursor, $segment)
        $item = Get-Item -LiteralPath $cursor -Force -ErrorAction SilentlyContinue
        if ($null -ne $item -and ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Reparse points are not accepted in package or install paths: $cursor"
        }
    }
}

function Get-Sha256([string] $Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Assert-Hash([object] $Value, [string] $Field) {
    if ($Value -isnot [string] -or $Value -cnotmatch '\A[0-9a-f]{64}\z') {
        throw "Invalid lowercase SHA-256 field: $Field"
    }
}

function Get-ResourceSpecFiles([System.Collections.IDictionary] $Spec) {
    if (-not $Spec.Contains('resource_coordinate') -or $null -eq $Spec['resource_coordinate']) {
        return @()
    }
    return @($Spec.resource_files)
}

function Get-AdditionalResourceSpecGroups([System.Collections.IDictionary] $Spec) {
    if (-not $Spec.Contains('additional_resource_groups')) { return @() }
    return @($Spec.additional_resource_groups)
}

function Assert-ResourceManifest(
    [object] $Manifest,
    [System.Collections.IDictionary] $Spec,
    [string] $Label,
    [switch] $AllowLegacyMissingAdditionalGroups
) {
    $expectedGroups = @(Get-AdditionalResourceSpecGroups $Spec)
    $hasResources = $Manifest.Contains('resources') -and $null -ne $Manifest['resources']
    if (-not $hasResources) {
        if ($expectedGroups.Count -eq 0 -and @(Get-ResourceSpecFiles $Spec).Count -eq 0) { return $null }
        throw "$Label is missing its pinned Antigravity bridge resource group."
    }
    $resources = $Manifest.resources
    if ($resources -isnot [System.Collections.IDictionary] -or $resources.schema_version -ne 1) {
        throw "$Label does not carry a supported resource record."
    }
    if ($expectedGroups.Count -eq 0) {
        throw "$Label advertises undeclared package resources."
    }
    # A manifest that still carries the retired OpenCode private server
    # coordinate is historical evidence only. It is accepted solely as
    # fenced read-only inventory (legacy switch below); the new-package gate
    # refuses to stage or install it anew.
    $isLegacyServerManifest = $resources.Contains('coordinate') -or $resources.Contains('dependency_policy')
    if ($isLegacyServerManifest) {
        if (-not $AllowLegacyMissingAdditionalGroups) {
            throw "$Label carries the retired OpenCode private server resource coordinate. New packages must not bundle the retired server; historical manifests remain read-only inventory."
        }
        $retiredCoordinate = 'swarm-kernel-host-opencode-resources'
        $retiredRepositoryRoot = 'modules/opencode'
        $retiredInstalledRoot = 'resources/modules/opencode'
        $retiredFiles = @(
            [ordered]@{ path = 'serve.mjs'; role = 'server_program' },
            [ordered]@{ path = 'native-mcp-proof.mjs'; role = 'plugin_module' },
            [ordered]@{ path = 'index.mjs'; role = 'plugin_entry' },
            [ordered]@{ path = 'package.json'; role = 'dependency_manifest' },
            [ordered]@{ path = 'package-lock.json'; role = 'dependency_lock' }
        )
        if ($resources.coordinate -cne $retiredCoordinate -or
            $resources.repository_relative_root -cne $retiredRepositoryRoot -or
            $resources.installed_relative_root -cne $retiredInstalledRoot -or
            $resources.files -isnot [array] -or
            $resources.dependency_policy -isnot [System.Collections.IDictionary]) {
            throw "$Label does not carry the pinned retired kernel resource coordinate."
        }
        $retiredPolicy = $resources.dependency_policy
        $retiredArguments = @('ci', '--ignore-scripts', '--no-audit', '--no-fund', '--no-progress', '--loglevel=error')
        $actualRetiredArguments = @($retiredPolicy.arguments | ForEach-Object { [string]$_ })
        if ($retiredPolicy.node_modules -cne 'installer_generated_locked_closure' -or
            $retiredPolicy.manager -cne 'npm' -or
            $retiredPolicy.install_command -cne 'npm ci --ignore-scripts' -or
            $retiredPolicy.output_directory -cne 'node_modules' -or
            $retiredPolicy.closure_manifest_file -cne 'dependency-closure.json' -or
            [long]$retiredPolicy.max_files -ne 100000 -or
            [long]$retiredPolicy.max_entries -ne 200000 -or
            [long]$retiredPolicy.max_total_bytes -ne 1073741824 -or
            [long]$retiredPolicy.max_manifest_bytes -ne 33554432 -or
            [long]$retiredPolicy.timeout_seconds -ne 1800 -or
            $actualRetiredArguments.Count -ne $retiredArguments.Count) {
            throw "$Label has an unsupported retired locked OpenCode dependency-install contract."
        }
        for ($index = 0; $index -lt $retiredArguments.Count; $index++) {
            if ($actualRetiredArguments[$index] -cne $retiredArguments[$index]) {
                throw "$Label has a different retired locked OpenCode dependency-install command."
            }
        }
        $actual = @($resources.files)
        if ($actual.Count -ne $retiredFiles.Count) { throw "$Label has an incomplete retired pinned kernel resource set." }
        for ($index = 0; $index -lt $retiredFiles.Count; $index++) {
            $row = $actual[$index]
            $entry = $retiredFiles[$index]
            $expectedFile = $retiredInstalledRoot + '/' + [string]$entry.path
            if ($row -isnot [System.Collections.IDictionary] -or
                [string]$row.path -cne [string]$entry.path -or
                [string]$row.role -cne [string]$entry.role -or
                [string]$row.file -cne $expectedFile -or
                [long]$row.bytes -le 0) {
                throw "$Label retired kernel resource row $index does not match the pinned coordinate."
            }
            Assert-Hash $row.source_sha256 "$Label.resources.files[$index].source_sha256"
            Assert-Hash $row.artifact_sha256 "$Label.resources.files[$index].artifact_sha256"
            if ([string]$row.source_sha256 -cne [string]$row.artifact_sha256) {
                throw "$Label retired kernel resource row $index has different source and artifact digests."
            }
        }
        # Strict-mode safe: a legacy build manifest has no 'additional_groups' key at all.
        $legacyGroups = @()
        if ($resources.Contains('additional_groups')) {
            $legacyGroups = @($resources['additional_groups'] | Where-Object { $null -ne $_ })
        }
        if ($legacyGroups.Count -eq 0) {
            # Inventory of an already installed package trusts its own build-manifest
            # identity: a legacy host staged before the Antigravity group existed
            # declares no additional groups and must not demand current group files.
            # New packages never take this branch (rejected above), so the exact
            # group requirement below stays mandatory for them.
            return $resources
        }
    } else {
        # New Antigravity-only shape: no retired server field may be present.
        foreach ($retiredField in @('coordinate', 'repository_relative_root', 'installed_relative_root', 'files', 'dependency_policy')) {
            if ($resources.Contains($retiredField) -and $null -ne $resources[$retiredField]) {
                throw "$Label mixes the retired OpenCode server resource field '$retiredField' with the Antigravity-only package shape."
            }
        }
    }
    # Shared exact Antigravity bridge group requirement: mandatory for new
    # packages; legacy inventory with groups validates them identically.
    # Strict-mode safe: a new or group-less legacy record may lack the key.
    $actualGroups = @()
    if ($resources.Contains('additional_groups')) {
        $actualGroups = @($resources['additional_groups'] | Where-Object { $null -ne $_ })
    }
    if ($actualGroups.Count -ne $expectedGroups.Count) {
        throw "$Label has an incomplete additional resource group set."
    }
    for ($groupIndex = 0; $groupIndex -lt $expectedGroups.Count; $groupIndex++) {
        $expectedGroup = $expectedGroups[$groupIndex]
        $actualGroup = $actualGroups[$groupIndex]
        if ($actualGroup -isnot [System.Collections.IDictionary] -or
            [string]$actualGroup.coordinate -cne [string]$expectedGroup.coordinate -or
            [string]$actualGroup.module_id -cne [string]$expectedGroup.module_id -or
            [string]$actualGroup.artifact_id -cne [string]$expectedGroup.artifact_id -or
            [string]$actualGroup.version -cne [string]$expectedGroup.version -or
            [string]$actualGroup.repository_relative_root -cne [string]$expectedGroup.repository_relative_root -or
            [string]$actualGroup.installed_relative_root -cne [string]$expectedGroup.installed_relative_root -or
            $actualGroup.files -isnot [array]) {
            throw "$Label additional resource group $groupIndex differs from the package policy."
        }
        $expectedGroupFiles = @($expectedGroup.resource_files)
        $actualGroupFiles = @($actualGroup.files)
        if ($actualGroupFiles.Count -ne $expectedGroupFiles.Count) {
            throw "$Label additional resource group $groupIndex is incomplete."
        }
        for ($fileIndex = 0; $fileIndex -lt $expectedGroupFiles.Count; $fileIndex++) {
            $row = $actualGroupFiles[$fileIndex]
            $entry = $expectedGroupFiles[$fileIndex]
            $expectedFile = [string]$expectedGroup.installed_relative_root + '/' + [string]$entry.path
            if ($row -isnot [System.Collections.IDictionary] -or
                [string]$row.path -cne [string]$entry.path -or
                [string]$row.role -cne [string]$entry.role -or
                [string]$row.file -cne $expectedFile -or
                [long]$row.bytes -le 0) {
                throw "$Label additional resource group $groupIndex file row $fileIndex differs from the package policy."
            }
            Assert-Hash $row.source_sha256 "$Label.resources.additional_groups[$groupIndex].files[$fileIndex].source_sha256"
            Assert-Hash $row.artifact_sha256 "$Label.resources.additional_groups[$groupIndex].files[$fileIndex].artifact_sha256"
            if ([string]$row.source_sha256 -cne [string]$row.artifact_sha256) {
                throw "$Label additional resource group $groupIndex file row $fileIndex has different source and artifact digests."
            }
        }
    }
    return $resources
}

function Assert-InstalledResourceRows(
    [string] $ResourceRoot,
    [string] $InstallRoot,
    [object[]] $Rows,
    [string] $Label
) {
    foreach ($row in $Rows) {
        $path = [IO.Path]::GetFullPath((Join-Path $ResourceRoot ([string]$row.path)))
        if (-not (Test-PathWithin $path $ResourceRoot) -or
            -not (Test-PathWithin $path $InstallRoot) -or
            -not (Test-Path -LiteralPath $path -PathType Leaf)) {
            throw "$Label is missing pinned resource '$($row.path)'."
        }
        Assert-NoReparseTraversal $path
        $item = Get-Item -LiteralPath $path -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or
            [long]$item.Length -ne [long]$row.bytes -or
            (Get-Sha256 $path) -cne [string]$row.artifact_sha256) {
            throw "$Label resource '$($row.path)' differs from its pinned digest or size."
        }
    }
}

function Assert-InstalledResourceFiles(
    [string] $InstallRoot,
    [System.Collections.IDictionary] $Spec,
    [object] $Resources,
    [string] $Label
) {
    if ($null -eq $Resources) { return }
    # Primary server rows exist only on retired historical manifests; their
    # install root comes from the manifest's own validated identity, never
    # from live policy. New Antigravity-only records carry groups alone.
    if ($Resources.Contains('files') -and $null -ne $Resources['files']) {
        $resourceRoot = [IO.Path]::GetFullPath((Join-Path $InstallRoot ([string]$Resources.installed_relative_root)))
        Assert-InstalledResourceRows -ResourceRoot $resourceRoot -InstallRoot $InstallRoot -Rows @($Resources.files) -Label $Label
    }
    $installedGroups = @()
    if ($Resources.Contains('additional_groups')) {
        $installedGroups = @($Resources['additional_groups'] | Where-Object { $null -ne $_ })
    }
    foreach ($group in $installedGroups) {
        $additionalRoot = [IO.Path]::GetFullPath((Join-Path $InstallRoot ([string]$group.installed_relative_root)))
        Assert-InstalledResourceRows -ResourceRoot $additionalRoot -InstallRoot $InstallRoot -Rows @($group.files) -Label "$Label $($group.coordinate)"
    }
}

# Retired: the OpenCode private SDK/server locked npm installation
# (Invoke-BoundedInstallerProcess, Get-PinnedNpmTools, Invoke-LockedNpmInstall,
# New-DependencyClosureManifest) is removed. New Antigravity-only packages
# stage no node_modules and need no Node/npm toolchain. Historical installs
# are verified read-only by Assert-DependencyClosure with the retained
# Get-DependencyTreeRows / Get-DependencyTreeSha256 helpers below; nothing
# here launches npm, Node, or a native server.

function Get-DependencyTreeRows([string] $NodeModulesPath, [System.Collections.IDictionary] $Policy) {
    if (-not (Test-Path -LiteralPath $NodeModulesPath -PathType Container)) {
        throw 'The installed OpenCode node_modules directory is missing.'
    }
    $rootItem = Get-Item -LiteralPath $NodeModulesPath -Force
    if (($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw 'The installed OpenCode node_modules root must not be a reparse point.'
    }
    $root = [IO.Path]::GetFullPath($NodeModulesPath)
    $pending = [Collections.Generic.Stack[string]]::new()
    $pending.Push($root)
    $entries = [Collections.Generic.List[object]]::new()
    $entryCount = [long]0
    $totalBytes = [long]0
    while ($pending.Count -gt 0) {
        $directory = $pending.Pop()
        foreach ($item in Get-ChildItem -LiteralPath $directory -Force) {
            $entryCount++
            if ($entryCount -gt [long]$Policy.max_entries) {
                throw 'Locked npm output exceeded its pinned directory-entry bound.'
            }
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw 'Locked npm output contains a symbolic link or reparse point.'
            }
            if ($item.PSIsContainer) {
                $pending.Push($item.FullName)
                continue
            }
            if (-not $item.PSIsContainer -and $item.Length -lt 0) {
                throw 'Locked npm output contains an invalid file entry.'
            }
            if ($entries.Count -ge [long]$Policy.max_files -or
                [long]$item.Length -gt ([long]$Policy.max_total_bytes - $totalBytes)) {
                throw 'Locked npm output exceeded its pinned file-count or total-size bound.'
            }
            $relative = [IO.Path]::GetRelativePath($root, $item.FullName).Replace('\', '/')
            if ([string]::IsNullOrWhiteSpace($relative) -or $relative.Length -gt 4096 -or
                $relative.StartsWith('/') -or $relative.Contains('\') -or $relative.Contains(':') -or
                @($relative.Split('/') | Where-Object { [string]::IsNullOrEmpty($_) -or $_ -in @('.', '..') }).Count -gt 0) {
                throw 'Locked npm output contains an unsafe relative path.'
            }
            $totalBytes += [long]$item.Length
            $utf8Path = [Text.UTF8Encoding]::new($false).GetBytes($relative)
            $entries.Add([pscustomobject]@{
                sort_key = [Convert]::ToHexString($utf8Path)
                path = $relative
                bytes = [long]$item.Length
                sha256 = Get-Sha256 $item.FullName
            })
        }
    }
    if ($entries.Count -eq 0) { throw 'Locked npm output contains no dependency files.' }
    $ordered = @($entries | Sort-Object -Property sort_key -CaseSensitive)
    return [pscustomobject]@{
        files = @($ordered | ForEach-Object {
            [ordered]@{ path = [string]$_.path; bytes = [long]$_.bytes; sha256 = [string]$_.sha256 }
        })
        entry_count = $entryCount
        total_bytes = $totalBytes
    }
}

function Get-DependencyTreeSha256([object[]] $Files) {
    $hasher = [Security.Cryptography.IncrementalHash]::CreateHash([Security.Cryptography.HashAlgorithmName]::SHA256)
    $utf8 = [Text.UTF8Encoding]::new($false)
    try {
        foreach ($file in $Files) {
            $record = [string]$file.path + [char]0 + [string]([long]$file.bytes) + [char]0 + [string]$file.sha256 + "`n"
            $bytes = $utf8.GetBytes($record)
            $hasher.AppendData($bytes)
        }
        return [Convert]::ToHexString($hasher.GetHashAndReset()).ToLowerInvariant()
    } finally { $hasher.Dispose() }
}

function Assert-DependencyClosure(
    [string] $ResourceRoot,
    [System.Collections.IDictionary] $ReceiptResources,
    [object] $BuildResources,
    [System.Collections.IDictionary] $Spec,
    [string] $Label
) {
    # Fenced read-only verification of historical installs only. The retired
    # locked-install bounds below are pinned literals (ex-policy), never live
    # configuration; no npm or Node process is launched. $Spec is accepted
    # for caller compatibility and otherwise unused.
    $policy = [ordered]@{
        output_directory = 'node_modules'
        closure_manifest_file = 'dependency-closure.json'
        max_files = 100000
        max_entries = 200000
        max_total_bytes = 1073741824
        max_manifest_bytes = 33554432
    }
    $retiredCoordinate = 'swarm-kernel-host-opencode-resources'
    $pin = $ReceiptResources.dependency_closure
    if ($pin -isnot [System.Collections.IDictionary] -or
        $pin.schema_version -ne 1 -or
        $pin.manifest_file -cne [string]$policy.closure_manifest_file -or
        $pin.coordinate -cne $retiredCoordinate) {
        throw "$Label has no pinned installed dependency closure receipt."
    }
    Assert-Hash $pin.manifest_sha256 "$Label.resources.dependency_closure.manifest_sha256"
    $manifestPath = Join-Path $ResourceRoot ([string]$pin.manifest_file)
    Assert-NoReparseTraversal $manifestPath
    if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf) -or
        (Get-Item -LiteralPath $manifestPath -Force).Length -gt [long]$policy.max_manifest_bytes -or
        (Get-Sha256 $manifestPath) -cne [string]$pin.manifest_sha256) {
        throw "$Label dependency closure manifest is missing or differs from its install receipt."
    }
    $manifest = Read-JsonMap $manifestPath "$Label dependency closure manifest"
    if ($manifest.schema_version -ne 1 -or
        $manifest.format -cne 'eliot.opencode_dependency_closure.v1' -or
        $manifest.coordinate -cne $retiredCoordinate -or
        $manifest.node -isnot [System.Collections.IDictionary] -or
        $manifest.npm -isnot [System.Collections.IDictionary] -or
        $manifest.files -isnot [array]) {
        throw "$Label dependency closure manifest has an unsupported format or coordinate."
    }
    foreach ($field in @('package_json_sha256', 'package_lock_sha256', 'tree_sha256')) {
        if ([string]$manifest[$field] -cne [string]$pin[$field]) {
            throw "$Label dependency closure identity differs from its install receipt."
        }
    }
    $toolIdentity = [ordered]@{
        node_version = $manifest.node.version
        node_executable_sha256 = $manifest.node.executable_sha256
        node_file = $manifest.node.file
        npm_version = $manifest.npm.version
        npm_cli_sha256 = $manifest.npm.cli_sha256
        npm_cli_file = $manifest.npm.file
    }
    foreach ($field in $toolIdentity.Keys) {
        if ([string]$toolIdentity[$field] -cne [string]$pin[$field]) {
            throw "$Label dependency closure tool identity differs from its install receipt."
        }
    }
    foreach ($field in @('node_executable_sha256', 'npm_cli_sha256', 'package_json_sha256', 'package_lock_sha256', 'tree_sha256')) {
        Assert-Hash $pin[$field] "$Label.resources.dependency_closure.$field"
    }
    if ([long]$manifest.entry_count -ne [long]$pin.entry_count -or
        [long]$manifest.entry_count -le 0 -or
        [long]$manifest.entry_count -gt [long]$policy.max_entries -or
        [long]$manifest.file_count -ne [long]$pin.file_count -or
        [long]$manifest.total_bytes -ne [long]$pin.total_bytes -or
        [long]$manifest.file_count -le 0 -or
        [long]$manifest.file_count -gt [long]$policy.max_files -or
        [long]$manifest.total_bytes -gt [long]$policy.max_total_bytes) {
        throw "$Label dependency closure exceeds or mismatches its pinned bounds."
    }
    if ((Get-Sha256 (Join-Path $ResourceRoot 'package.json')) -cne [string]$pin.package_json_sha256 -or
        (Get-Sha256 (Join-Path $ResourceRoot 'package-lock.json')) -cne [string]$pin.package_lock_sha256) {
        throw "$Label package files differ from the locked dependency closure."
    }
    $tree = Get-DependencyTreeRows (Join-Path $ResourceRoot ([string]$policy.output_directory)) $policy
    if ($tree.files.Count -ne [long]$manifest.file_count -or
        [long]$tree.entry_count -ne [long]$manifest.entry_count -or
        [long]$tree.total_bytes -ne [long]$manifest.total_bytes -or
        (Get-DependencyTreeSha256 @($tree.files)) -cne [string]$manifest.tree_sha256) {
        throw "$Label installed node_modules differs from its dependency closure manifest."
    }
    if ($manifest.files.Count -ne $tree.files.Count) {
        throw "$Label installed node_modules has a different number of files than its manifest."
    }
    for ($index = 0; $index -lt $tree.files.Count; $index++) {
        $actual = $tree.files[$index]
        $expected = $manifest.files[$index]
        if ([string]$actual.path -cne [string]$expected.path -or
            [long]$actual.bytes -ne [long]$expected.bytes -or
            [string]$actual.sha256 -cne [string]$expected.sha256) {
            throw "$Label installed node_modules file rows differ from the pinned manifest."
        }
    }
}

function Assert-ReceiptResourceContract([object] $Receipt, [object] $Resources, [string] $Label) {
    if ($null -eq $Resources) {
        if ($null -ne $Receipt.resources) { throw "$Label receipt advertises undeclared package resources." }
        return
    }
    $receiptResources = $Receipt.resources
    if ($receiptResources -isnot [System.Collections.IDictionary]) {
        throw "$Label receipt does not carry a supported resource record."
    }
    $isLegacyManifest = ($Resources.Contains('coordinate') -and $null -ne $Resources['coordinate']) -or
        ($Resources.Contains('dependency_policy') -and $null -ne $Resources['dependency_policy'])
    $isLegacyReceipt = ($receiptResources.Contains('coordinate') -and $null -ne $receiptResources['coordinate']) -or
        ($receiptResources.Contains('dependency_closure') -and $null -ne $receiptResources['dependency_closure'])
    if ($isLegacyManifest -ne $isLegacyReceipt) {
        throw "$Label receipt resource shape does not match its build manifest."
    }
    if ($isLegacyManifest) {
        # Fenced historical receipt: retired coordinate literals, never live policy.
        if ($receiptResources.coordinate -cne 'swarm-kernel-host-opencode-resources' -or
            $receiptResources.installed_relative_root -cne 'resources/modules/opencode' -or
            $receiptResources.files -isnot [array]) {
            throw "$Label receipt does not carry the retired kernel resource coordinate."
        }
        $manifestRows = @($Resources.files)
        $receiptRows = @($receiptResources.files)
        if ($manifestRows.Count -ne $receiptRows.Count) { throw "$Label receipt has an incomplete retired kernel resource set." }
        for ($index = 0; $index -lt $manifestRows.Count; $index++) {
            if ([string]$receiptRows[$index].path -cne [string]$manifestRows[$index].path -or
                [string]$receiptRows[$index].file -cne [string]$manifestRows[$index].file -or
                [string]$receiptRows[$index].artifact_sha256 -cne [string]$manifestRows[$index].artifact_sha256) {
                throw "$Label receipt retired kernel resource row $index does not match its build manifest."
            }
        }
        $closure = $receiptResources.dependency_closure
        if ($closure -isnot [System.Collections.IDictionary] -or
            $closure.schema_version -ne 1 -or
            $closure.manifest_file -cne 'dependency-closure.json' -or
            $closure.coordinate -cne 'swarm-kernel-host-opencode-resources') {
            throw "$Label receipt does not pin its installed dependency closure."
        }
        foreach ($field in @('manifest_sha256', 'package_json_sha256', 'package_lock_sha256', 'tree_sha256', 'node_executable_sha256', 'npm_cli_sha256')) {
            Assert-Hash $closure[$field] "$Label receipt dependency_closure.$field"
        }
        if ([long]$closure.entry_count -le 0 -or [long]$closure.entry_count -gt 200000) {
            throw "$Label receipt dependency closure has an invalid directory-entry count."
        }
    }
    # Antigravity bridge groups are pinned on both new and grouped legacy
    # receipts; legacy receipts predating the group carry none and skip this.
    $manifestGroups = @()
    if ($Resources.Contains('additional_groups')) {
        $manifestGroups = @($Resources['additional_groups'] | Where-Object { $null -ne $_ })
    }
    $receiptGroups = @()
    if ($receiptResources.Contains('additional_groups')) {
        $receiptGroups = @($receiptResources['additional_groups'] | Where-Object { $null -ne $_ })
    }
    if ($receiptGroups.Count -ne $manifestGroups.Count) {
        if ($isLegacyManifest -and $receiptGroups.Count -eq 0) { return }
        throw "$Label receipt has an incomplete additional resource group set."
    }
    for ($groupIndex = 0; $groupIndex -lt $manifestGroups.Count; $groupIndex++) {
        $manifestGroup = $manifestGroups[$groupIndex]
        $receiptGroup = $receiptGroups[$groupIndex]
        if ($receiptGroup -isnot [System.Collections.IDictionary] -or
            [string]$receiptGroup.coordinate -cne [string]$manifestGroup.coordinate -or
            [string]$receiptGroup.installed_relative_root -cne [string]$manifestGroup.installed_relative_root) {
            throw "$Label receipt additional resource group $groupIndex does not match its build manifest."
        }
        $manifestFiles = @($manifestGroup.files)
        $receiptFiles = @($receiptGroup.files)
        if ($manifestFiles.Count -ne $receiptFiles.Count) {
            throw "$Label receipt additional resource group $groupIndex is incomplete."
        }
        for ($fileIndex = 0; $fileIndex -lt $manifestFiles.Count; $fileIndex++) {
            if ([string]$receiptFiles[$fileIndex].path -cne [string]$manifestFiles[$fileIndex].path -or
                [string]$receiptFiles[$fileIndex].file -cne [string]$manifestFiles[$fileIndex].file -or
                [string]$receiptFiles[$fileIndex].artifact_sha256 -cne [string]$manifestFiles[$fileIndex].artifact_sha256) {
                throw "$Label receipt additional resource group $groupIndex file row $fileIndex does not match its build manifest."
            }
        }
    }
}

function Assert-DependencyPins([object] $Manifest, [string] $Label) {
    $pins = @($Manifest.dependency_pins)
    if ($pins.Count -eq 0) {
        throw "$Label has no resolved package/transitive source pins. Repackage the exact selected artifact with the updated provenance builder."
    }
    $coordinates = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($pin in $pins) {
        if ($pin -isnot [System.Collections.IDictionary] -or
            [string]$pin.name -cnotmatch '\A[A-Za-z0-9][A-Za-z0-9_-]{0,127}\z' -or
            [string]$pin.version -cnotmatch '\A[0-9A-Za-z.+-]{1,128}\z' -or
            [string]::IsNullOrWhiteSpace([string]$pin.source) -or
            $pin.features -isnot [array]) {
            throw "$Label contains a malformed resolved dependency pin."
        }
        $coordinate = [string]$pin.name + "`0" + [string]$pin.version + "`0" + [string]$pin.source
        if (-not $coordinates.Add($coordinate)) { throw "$Label repeats a resolved dependency coordinate." }
        if ([string]$pin.source -ceq 'path') {
            if ([string]$pin.manifest -notmatch '\A[A-Za-z0-9._/-]+\z' -or
                [string]$pin.manifest -match '(^|/)\.\.(/|$)') {
                throw "$Label contains an unsafe path dependency manifest pin."
            }
            Assert-Hash $pin.manifest_sha256 "$Label.dependency_pins.manifest_sha256"
            Assert-Hash $pin.source_tree_sha256 "$Label.dependency_pins.source_tree_sha256"
        } elseif ($null -ne $pin['checksum']) {
            Assert-Hash $pin.checksum "$Label.dependency_pins.checksum"
        } elseif ([string]$pin.source -notmatch '#[0-9a-f]{40,64}\z') {
            throw "$Label contains an external package without a content checksum or pinned Git revision."
        }
    }
}

function Get-HostIpcVersion([object] $Manifest, [string] $Label) {
    $value = $Manifest.compatibility.host_ipc.protocol_version
    $text = [string]$value
    $version = 0
    if ($text -cnotmatch '\A[1-9][0-9]{0,4}\z' -or
        -not [int]::TryParse($text, [ref]$version) -or $version -gt 65535) {
        throw "$Label has no supported explicit host client.hello protocol_version. Rebuild it with the current package provenance builder."
    }
    return $version
}

function Get-HostRuntimeCoordinate([object] $Manifest, [string] $Label, [switch] $AllowLegacyServerResources) {
    $runtime = $Manifest.compatibility.host_runtime
    if ($runtime -isnot [System.Collections.IDictionary] -or
        $runtime.package_name -cne 'swarm-kernel-host' -or
        $runtime.manifest -cne 'crates/swarm-kernel-host/Cargo.toml' -or
        $runtime.binary_target -cne 'swarm-kernel-host' -or
        $runtime.role -cne 'host_runtime') {
        throw "$Label does not pin the independent swarm-kernel-host runtime coordinate. Rebuild it with the current provenance builder."
    }
    # New packages carry no server resource requirement here. Historical
    # manifests pin the retired coordinate; accepted only as fenced
    # read-only inventory, never for new packages.
    $hasLegacyServerFields = ($runtime.Contains('resource_coordinate') -and $null -ne $runtime['resource_coordinate']) -or
        ($runtime.Contains('resource_installed_relative_root') -and $null -ne $runtime['resource_installed_relative_root'])
    if ($hasLegacyServerFields) {
        if (-not $AllowLegacyServerResources) {
            throw "$Label carries the retired OpenCode server resource requirement in its host runtime coordinate. Rebuild it with the current provenance builder."
        }
        if ($runtime.resource_coordinate -cne 'swarm-kernel-host-opencode-resources' -or
            $runtime.resource_installed_relative_root -cne 'resources/modules/opencode') {
            throw "$Label pins an unsupported retired server resource coordinate."
        }
    }
    return $runtime
}

function Get-HostSupervisorCoordinate([object] $Manifest, [string] $Label) {
    $supervisor = $Manifest.compatibility.host_supervisor
    if ($supervisor -isnot [System.Collections.IDictionary] -or
        $supervisor.package_name -cne 'swarm-supervisor' -or
        $supervisor.manifest -cne 'crates/swarm-supervisor/Cargo.toml' -or
        $supervisor.binary_target -cne 'swarm-supervisor' -or
        $supervisor.role -cne 'host_supervisor') {
        throw "$Label does not pin the standalone swarm-supervisor coordinate. Rebuild it with the current provenance builder."
    }
    return $supervisor
}

function Assert-CompatibilityManifest(
    [object] $Manifest,
    [string] $PackageName,
    [System.Collections.IDictionary] $Spec,
    [string] $Label,
    [switch] $AllowLegacyServerResources
) {
    if ($Manifest.compatibility -isnot [System.Collections.IDictionary]) {
        throw "$Label is missing its explicit host IPC/launcher compatibility record. Repackage it with the updated provenance builder."
    }
    [void](Get-HostIpcVersion $Manifest $Label)
    $runtime = $null
    $supervisor = $null
    if ($PackageName -in @('eliot-swarm-controller', 'swarm-kernel-host', 'swarm-supervisor', 'swarm-cli')) {
        $runtime = Get-HostRuntimeCoordinate $Manifest $Label -AllowLegacyServerResources:$AllowLegacyServerResources
        $supervisor = Get-HostSupervisorCoordinate $Manifest $Label
    }
    $targetTriple = [string]$Manifest.compatibility.target.rustc_host_triple
    if ($targetTriple -cnotmatch '\A[A-Za-z0-9_-]{1,128}\z') {
        throw "$Label does not pin the executable target triple. Repackage it with the updated provenance builder."
    }
    $expectedSiblings = @($Spec.required_siblings | ForEach-Object { [string]$_ })
    $recordedSiblings = @($Manifest.compatibility.required_sibling_binaries | ForEach-Object { [string]$_ })
    if ($expectedSiblings.Count -ne $recordedSiblings.Count) {
        throw "$Label records incompatible required sibling binaries."
    }
    for ($index = 0; $index -lt $expectedSiblings.Count; $index++) {
        if ($recordedSiblings[$index] -cne $expectedSiblings[$index]) {
            throw "$Label records incompatible required sibling binaries."
        }
    }
    if ($PackageName -ceq 'eliot-swarm-controller') {
        if ([string]$Manifest.compatibility.process_role -cne 'host_launcher' -or
            @($recordedSiblings | Where-Object { $_ -ceq 'swarm-kernel-host' }).Count -ne 1) {
            throw "$Label must require the independent swarm-kernel-host sibling for the public swarm-host launcher."
        }
        $launcher = $Manifest.compatibility.gateway_launcher
        $arguments = @($launcher.required_arguments | ForEach-Object { [string]$_ })
        if ($launcher.package_name -cne 'swarm-gateway' -or
            $launcher.binary_target -cne 'swarm-gateway' -or
            $arguments.Count -ne 2 -or $arguments[0] -cne '--config' -or $arguments[1] -cne '--data-dir') {
            throw "$Label does not declare the observed swarm-gateway binary and argv contract. Rebuild the root swarm-host package with the updated provenance builder."
        }
    } elseif ($PackageName -ceq 'swarm-kernel-host') {
        if ([string]$Manifest.compatibility.process_role -cne 'host_runtime' -or
            @($recordedSiblings | Where-Object { $_ -ceq 'swarm-supervisor' }).Count -ne 1) {
            throw "$Label does not identify swarm-kernel-host as the standalone Store/IPC owner with its supervisor sibling."
        }
    } elseif ($PackageName -ceq 'swarm-supervisor') {
        if ([string]$Manifest.compatibility.process_role -cne 'host_supervisor' -or
            @($recordedSiblings).Count -ne 0) {
            throw "$Label does not identify swarm-supervisor as the standalone supervisor process."
        }
    } elseif ($PackageName -ceq 'swarm-cli') {
        $launcher = $Manifest.compatibility.host_launcher
        $runtime = Get-HostRuntimeCoordinate $Manifest $Label -AllowLegacyServerResources:$AllowLegacyServerResources
        if ($launcher.package_name -cne 'eliot-swarm-controller' -or
            $launcher.binary_target -cne 'swarm-host' -or
            $runtime.package_name -cne 'swarm-kernel-host' -or
            $supervisor.package_name -cne 'swarm-supervisor' -or
            @($Manifest.compatibility.required_sibling_binaries | Where-Object { [string]$_ -ceq 'eliot-swarm-controller' }).Count -ne 1) {
            throw "$Label does not declare its exact swarm-host launcher and swarm-kernel-host runtime coordinates. Rebuild the public CLI with the updated provenance builder."
        }
    } elseif ($PackageName -ceq 'swarm-gateway') {
        $arguments = @($Manifest.compatibility.accepted_launcher_arguments | ForEach-Object { [string]$_ })
        foreach ($required in @('--config', '--data-dir')) {
            if (@($arguments | Where-Object { $_ -ceq $required }).Count -ne 1) {
                throw "$Label does not advertise the '$required' option forwarded by the root swarm-host launcher. Rebuild the gateway package with the updated provenance builder."
            }
        }
    }
}

function Assert-HostIpcCompatible([object] $Left, [string] $LeftLabel, [object] $Right, [string] $RightLabel) {
    $leftVersion = Get-HostIpcVersion $Left $LeftLabel
    $rightVersion = Get-HostIpcVersion $Right $RightLabel
    $leftTarget = [string]$Left.compatibility.target.rustc_host_triple
    $rightTarget = [string]$Right.compatibility.target.rustc_host_triple
    # Inventory context: either side may be a historical manifest that still
    # pins the retired server resource requirement.
    $leftRuntime = if ($null -ne $Left.compatibility['host_runtime']) { Get-HostRuntimeCoordinate $Left $LeftLabel -AllowLegacyServerResources } else { $null }
    $rightRuntime = if ($null -ne $Right.compatibility['host_runtime']) { Get-HostRuntimeCoordinate $Right $RightLabel -AllowLegacyServerResources } else { $null }
    $leftSupervisor = if ($null -ne $Left.compatibility['host_supervisor']) { Get-HostSupervisorCoordinate $Left $LeftLabel } else { $null }
    $rightSupervisor = if ($null -ne $Right.compatibility['host_supervisor']) { Get-HostSupervisorCoordinate $Right $RightLabel } else { $null }
    # Retired server resource fields are compared only when both sides pin
    # them: a mixed historical/new pair differs by design, not by contract.
    $leftLegacyServer = $null -ne $leftRuntime -and
        ($leftRuntime.Contains('resource_coordinate') -and $null -ne $leftRuntime['resource_coordinate'])
    $rightLegacyServer = $null -ne $rightRuntime -and
        ($rightRuntime.Contains('resource_coordinate') -and $null -ne $rightRuntime['resource_coordinate'])
    $serverMismatch = $leftLegacyServer -and $rightLegacyServer -and
        ([string]$leftRuntime['resource_coordinate'] -cne [string]$rightRuntime['resource_coordinate'] -or
         [string]$leftRuntime['resource_installed_relative_root'] -cne [string]$rightRuntime['resource_installed_relative_root'])
    $runtimeMismatch = $null -ne $leftRuntime -and $null -ne $rightRuntime -and
        ($leftRuntime.package_name -cne $rightRuntime.package_name -or
         $leftRuntime.binary_target -cne $rightRuntime.binary_target -or
         $serverMismatch)
    $supervisorMismatch = $null -ne $leftSupervisor -and $null -ne $rightSupervisor -and
        ($leftSupervisor.package_name -cne $rightSupervisor.package_name -or
         $leftSupervisor.binary_target -cne $rightSupervisor.binary_target -or
         $leftSupervisor.manifest -cne $rightSupervisor.manifest)
    if ($leftVersion -ne $rightVersion -or $leftTarget -cne $rightTarget -or
        $runtimeMismatch -or $supervisorMismatch) {
        throw "Frontend runtime contract mismatch: $LeftLabel uses client.hello v$leftVersion / $leftTarget while $RightLabel uses v$rightVersion / $rightTarget. Install compatible versioned frontend artifacts together."
    }
}

function Assert-GatewayLauncherCompatible([object] $HostManifest, [object] $GatewayManifest) {
    $launcher = $HostManifest.compatibility.gateway_launcher
    if ($launcher.package_name -cne 'swarm-gateway' -or $launcher.binary_target -cne 'swarm-gateway') {
        throw 'The root swarm-host package does not name the actual swarm-gateway sibling binary.'
    }
    $required = @($launcher.required_arguments | ForEach-Object { [string]$_ })
    $accepted = @($GatewayManifest.compatibility.accepted_launcher_arguments | ForEach-Object { [string]$_ })
    foreach ($argument in $required) {
        if (@($accepted | Where-Object { $_ -ceq $argument }).Count -ne 1) {
            throw "The installed swarm-gateway manifest does not declare root swarm-host's required '$argument' argument. Install a compatible gateway artifact into a new versioned directory; no files were changed."
        }
    }
}

function Read-JsonMap([string] $Path, [string] $Label) {
    try {
        $value = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json -AsHashtable
    } catch {
        throw "$Label is not valid JSON: $Path"
    }
    if ($value -isnot [System.Collections.IDictionary]) { throw "$Label must be a JSON object." }
    return $value
}

function Assert-SourceIdentity([object] $Source, [string] $Prefix, [bool] $RequirePackageManifestHash) {
    if ($Source.commit -isnot [string] -or $Source.commit -cnotmatch '\A[0-9a-f]{40,64}\z' -or
        $Source.tree -isnot [string] -or $Source.tree -cnotmatch '\A[0-9a-f]{40,64}\z' -or
        $Source.checkout_clean_before -ne $true -or $Source.checkout_clean_after -ne $true) {
        throw "$Prefix must pin a clean exact Git commit and tree."
    }
    foreach ($field in @('cargo_toml_sha256', 'cargo_lock_sha256', 'rust_toolchain_toml_sha256')) {
        Assert-Hash $Source[$field] "$Prefix.$field"
    }
    Assert-Hash $Source.package_manifest_sha256 "$Prefix.package_manifest_sha256"
    if ($null -ne $Source['workspace_cargo_toml_sha256']) {
        Assert-Hash $Source.workspace_cargo_toml_sha256 "$Prefix.workspace_cargo_toml_sha256"
        if ([string]$Source.workspace_cargo_toml_sha256 -cne [string]$Source.cargo_toml_sha256) {
            throw "$Prefix workspace Cargo.toml aliases do not match."
        }
    } elseif ($RequirePackageManifestHash) {
        throw "$Prefix is missing the explicit workspace Cargo.toml pin."
    }
}

function Get-InstalledFrontend(
    [string] $Name,
    [string] $Root,
    [System.Collections.IDictionary] $Spec,
    [string] $RequestedPackage
) {
    $binaryLeaf = [string]$Spec.binary + '.exe'
    $binaryPath = Join-Path $Root $binaryLeaf
    $buildPath = Join-Path $Root ([string]$Spec.binary + '.build-manifest.json')
    $receiptPath = Join-Path $Root ([string]$Spec.binary + '.install-receipt.json')
    # Identity sidecars decide whether the package is installed at all. Resource
    # files are required by the installed build manifest's own identity (read
    # below), so a legacy host without the current Antigravity group is not
    # forced to carry current group files here.
    $identityPaths = @($binaryPath, $buildPath, $receiptPath)
    $identityPresent = @($identityPaths | Where-Object { Test-Path -LiteralPath $_ })
    if ($identityPresent.Count -eq 0) {
        # Preserve the original partial-install detection: known resource files
        # without any identity sidecar are an orphaned install, not an absent one.
        # Only EXISTING spec-known paths are examined. The retired OpenCode
        # server files are intentionally not listed: they are never part of a
        # new install, so stray historical bytes without identity are not ours.
        $specResourcePaths = @(
            foreach ($group in @(Get-AdditionalResourceSpecGroups $Spec)) {
                foreach ($entry in @($group.resource_files)) {
                    Join-Path (Join-Path $Root ([string]$group.installed_relative_root)) ([string]$entry.path)
                }
            }
        )
        $orphans = @($specResourcePaths | Where-Object { Test-Path -LiteralPath $_ })
        if ($orphans.Count -gt 0) {
            throw "Installed frontend '$Name' is incomplete; missing $($identityPaths -join ', '). Found orphaned resource files without package identity: $($orphans -join ', '). Use a new empty install directory or restore the exact package files before installing another frontend."
        }
        return $null
    }
    if ($identityPresent.Count -ne $identityPaths.Count) {
        $missing = @($identityPaths | Where-Object { -not (Test-Path -LiteralPath $_) })
        if ($Name -ceq 'swarm-gateway' -and $RequestedPackage -ceq 'eliot-swarm-controller') {
            throw "The existing swarm gateway sibling is not provenance-complete; missing $($missing -join ', '). Use a new empty install directory, install the matching swarm-gateway package there first, then install the root swarm-host package."
        }
        throw "Installed frontend '$Name' is incomplete; missing $($missing -join ', '). Use a new empty install directory or restore the exact package files before installing another frontend."
    }
    foreach ($path in $identityPaths) {
        Assert-NoReparseTraversal $path
        $item = Get-Item -LiteralPath $path -Force
        if ($item.PSIsContainer -or $item.Length -le 0) { throw "Installed frontend file is not a non-empty regular file: $path" }
    }

    $receipt = Read-JsonMap $receiptPath 'Installed frontend receipt'
    $manifest = Read-JsonMap $buildPath 'Installed frontend build manifest'
    $isHostPackage = Test-HostPackage $Name
    $expectedManifestFormat = if ($isHostPackage) {
        'eliot.module_build_manifest.v1'
    } else {
        'eliot.frontend_build_manifest.v1'
    }
    $buildTargetValid = if ($isHostPackage) {
        @($manifest.build.binary_targets | Where-Object { [string]$_ -ceq [string]$Spec.binary }).Count -eq 1
    } else {
        $manifest.build.binary_target -ceq [string]$Spec.binary
    }
    if ($receipt.schema_version -ne 1 -or
        $receipt.format -cne 'eliot.frontend_install_receipt.v1' -or
        $receipt.package_name -cne $Name -or
        $receipt.role -cne [string]$Spec.role -or
        $receipt.binary_target -cne [string]$Spec.binary -or
        $receipt.binary_file -cne $binaryLeaf -or
        $receipt.build_manifest_file -cne ([string]$Spec.binary + '.build-manifest.json') -or
        $manifest.schema_version -ne 1 -or
        $manifest.format -cne $expectedManifestFormat -or
        (-not $isHostPackage -and $manifest.build.role -cne [string]$Spec.role) -or
        $manifest.build.package_name -cne $Name -or
        $manifest.build.package_manifest -cne [string]$Spec.manifest -or
        -not $buildTargetValid -or
        $manifest.build.profile -cne 'release' -or
        [string]$manifest.build.package_version -cne [string]$receipt.package_version) {
        throw "Installed frontend '$Name' has incompatible package identity metadata. Use a new empty install directory."
    }
    $expectedReceiptSiblings = @($Spec.required_siblings)
    $recordedReceiptSiblings = @($receipt.required_sibling_binaries | ForEach-Object { [string]$_ })
    if ($expectedReceiptSiblings.Count -ne $recordedReceiptSiblings.Count) {
        throw "Installed frontend '$Name' receipt has incompatible sibling requirements."
    }
    for ($index = 0; $index -lt $expectedReceiptSiblings.Count; $index++) {
        if ($recordedReceiptSiblings[$index] -cne [string]$expectedReceiptSiblings[$index]) {
            throw "Installed frontend '$Name' receipt has incompatible sibling requirements."
        }
    }
    Assert-SourceIdentity $manifest.source 'Installed build source' $isHostPackage
    Assert-DependencyPins $manifest "Installed frontend '$Name' build manifest"
    # Inventory context: already installed historical manifests may still pin
    # the retired server resource requirement; read-only verification only.
    Assert-CompatibilityManifest $manifest $Name $Spec "Installed frontend '$Name' build manifest" -AllowLegacyServerResources
    $resources = Assert-ResourceManifest $manifest $Spec "Installed frontend '$Name' build manifest" -AllowLegacyMissingAdditionalGroups
    if ($null -ne $resources) {
        # Required resource files come from the installed build manifest's own
        # identity. Retired server rows exist only on historical manifests
        # (rooted at their own validated relative root); new Antigravity-only
        # records list groups alone.
        $installedResourcePaths = @(
            if ($resources.Contains('files') -and $null -ne $resources['files']) {
                foreach ($row in @($resources.files)) {
                    Join-Path (Join-Path $Root ([string]$resources.installed_relative_root)) ([string]$row.path)
                }
            }
            $legacyGroups = @()
            if ($resources.Contains('additional_groups')) {
                $legacyGroups = @($resources['additional_groups'] | Where-Object { $null -ne $_ })
            }
            foreach ($group in $legacyGroups) {
                foreach ($row in @($group.files)) {
                    Join-Path (Join-Path $Root ([string]$group.installed_relative_root)) ([string]$row.path)
                }
            }
        )
        $installedResourcePresent = @($installedResourcePaths | Where-Object { Test-Path -LiteralPath $_ })
        if ($installedResourcePresent.Count -ne $installedResourcePaths.Count) {
            $missing = @($installedResourcePaths | Where-Object { -not (Test-Path -LiteralPath $_) })
            if ($Name -ceq 'swarm-gateway' -and $RequestedPackage -ceq 'eliot-swarm-controller') {
                throw "The existing swarm gateway sibling is not provenance-complete; missing $($missing -join ', '). Use a new empty install directory, install the matching swarm-gateway package there first, then install the root swarm-host package."
            }
            throw "Installed frontend '$Name' is incomplete; missing $($missing -join ', '). Use a new empty install directory or restore the exact package files before installing another frontend."
        }
    }
    Assert-ReceiptResourceContract $receipt $resources "Installed frontend '$Name'"
    Assert-InstalledResourceFiles $Root $Spec $resources "Installed frontend '$Name'"
    # The retired locked dependency closure is re-verified read-only on
    # historical installs only; new Antigravity-only records carry none.
    $hasLegacyServerRows = $null -ne $resources -and $resources.Contains('files') -and $null -ne $resources['files']
    $hasLegacyClosure = $null -ne $receipt.resources -and
        $receipt.resources.Contains('dependency_closure') -and $null -ne $receipt.resources['dependency_closure']
    if ($hasLegacyServerRows -and $hasLegacyClosure) {
        $installedResourceRoot = Join-Path $Root ([string]$resources.installed_relative_root)
        Assert-DependencyClosure $installedResourceRoot $receipt.resources $resources $Spec "Installed frontend '$Name'"
    } elseif ($hasLegacyClosure -xor $hasLegacyServerRows) {
        throw "Installed frontend '$Name' mixes retired server resources with an Antigravity-only install record. Use a new empty install directory."
    }
    Assert-Hash $receipt.package_manifest_sha256 'receipt.package_manifest_sha256'
    if ($null -ne $manifest.source['package_manifest_path'] -and
        [string]$manifest.source.package_manifest_path -cne [string]$manifest.build.package_manifest) {
        throw "Installed frontend '$Name' selected package-manifest path differs between build and source provenance."
    }
    $expectedPackageManifestHash = [string]$manifest.source.package_manifest_sha256
    if ([string]$receipt.package_manifest_sha256 -cne $expectedPackageManifestHash) {
        throw "Installed frontend '$Name' package-manifest pin does not match its source manifest."
    }
    $expectedWorkspaceManifestHash = if ($null -ne $manifest.source['workspace_cargo_toml_sha256']) {
        [string]$manifest.source.workspace_cargo_toml_sha256
    } else {
        [string]$manifest.source.cargo_toml_sha256
    }
    Assert-Hash $receipt.executable_sha256 'receipt.executable_sha256'
    Assert-Hash $receipt.build_manifest_sha256 'receipt.build_manifest_sha256'
    if ((Get-Sha256 $buildPath) -cne [string]$receipt.build_manifest_sha256 -or
        (Get-Sha256 $binaryPath) -cne [string]$receipt.executable_sha256 -or
        [string]$manifest.source.commit -cne [string]$receipt.source_commit -or
        [string]$manifest.source.tree -cne [string]$receipt.source_tree -or
        [string]$manifest.source.cargo_lock_sha256 -cne [string]$receipt.cargo_lock_sha256 -or
        [string]$manifest.source.cargo_toml_sha256 -cne [string]$receipt.cargo_toml_sha256 -or
        [string]$manifest.source.package_manifest_sha256 -cne [string]$receipt.package_manifest_sha256 -or
        $expectedWorkspaceManifestHash -cne [string]$receipt.workspace_cargo_toml_sha256 -or
        [string]$manifest.source.rust_toolchain_toml_sha256 -cne [string]$receipt.rust_toolchain_toml_sha256) {
        throw "Installed frontend '$Name' executable or provenance sidecar digest does not match its receipt. Use a new empty install directory."
    }
    $artifactRows = @($manifest.artifacts | Where-Object {
        [string]$_.target_name -ceq [string]$Spec.binary -and
        [string]$_.file -ceq ("bin/" + $binaryLeaf)
    })
    if ($artifactRows.Count -ne 1 -or
        [string]$artifactRows[0].source_sha256 -cne [string]$artifactRows[0].artifact_sha256 -or
        [string]$artifactRows[0].artifact_sha256 -cne [string]$receipt.executable_sha256 -or
        [long]$artifactRows[0].bytes -ne (Get-Item -LiteralPath $binaryPath).Length) {
        throw "Installed frontend '$Name' build manifest does not bind the installed executable."
    }
    return [pscustomobject]@{
        package_name = $Name
        role = [string]$Spec.role
        binary = $binaryPath
        build_manifest = $manifest
        build_manifest_sha256 = [string]$receipt.build_manifest_sha256
        executable_sha256 = [string]$receipt.executable_sha256
        source_commit = [string]$manifest.source.commit
        source_tree = [string]$manifest.source.tree
        cargo_toml_sha256 = [string]$manifest.source.cargo_toml_sha256
        cargo_lock_sha256 = [string]$manifest.source.cargo_lock_sha256
        rust_toolchain_toml_sha256 = [string]$manifest.source.rust_toolchain_toml_sha256
        rustc_version_verbose = [string]$manifest.toolchain.rustc_version_verbose
        cargo_version_verbose = [string]$manifest.toolchain.cargo_version_verbose
        pinned_channel = [string]$manifest.toolchain.pinned_channel
        resources = $resources
    }
}

function Copy-ToStage([string] $Source, [string] $Stage) {
    $inputStream = [IO.File]::Open($Source, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
    try {
        $outputStream = [IO.File]::Open($Stage, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
        try {
            $inputStream.CopyTo($outputStream)
            $outputStream.Flush($true)
        } finally { $outputStream.Dispose() }
    } finally { $inputStream.Dispose() }
}

$packagePath = Get-CanonicalPath $PackageDirectory 'PackageDirectory'
$installRoot = Get-CanonicalPath $InstallDirectory 'InstallDirectory'
Assert-NoReparseTraversal $packagePath
Assert-NoReparseTraversal $installRoot
if (-not (Test-Path -LiteralPath $packagePath -PathType Container)) { throw 'PackageDirectory must be an existing directory.' }
if (-not (Test-Path -LiteralPath $installRoot -PathType Container)) { throw 'InstallDirectory must already exist as a directory.' }
if ((Test-PathWithin $packagePath $installRoot) -or (Test-PathWithin $installRoot $packagePath)) {
    throw 'PackageDirectory and InstallDirectory must be separate and non-overlapping.'
}

$manifestPath = Join-Path $packagePath 'build-manifest.json'
if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
    throw 'PackageDirectory must come from Build-SwarmFrontendProvenance.ps1 and contain build-manifest.json.'
}
Assert-NoReparseTraversal $manifestPath
$provenance = Read-JsonMap $manifestPath 'Frontend build manifest'
$packageName = [string]$provenance.build.package_name
if (-not $packageMap.Contains($packageName)) {
    throw "Build manifest names unsupported package '$packageName'. Select swarm-mcp, swarm-gateway, swarm-cli, eliot-swarm-controller, swarm-supervisor, or swarm-kernel-host."
}
$selectedSpec = $packageMap[$packageName]
$isHostPackage = Test-HostPackage $packageName
$expectedManifestFormat = if ($isHostPackage) { 'eliot.module_build_manifest.v1' } else { 'eliot.frontend_build_manifest.v1' }
$selectedTargetValid = if ($isHostPackage) {
    @($provenance.build.binary_targets | Where-Object { [string]$_ -ceq [string]$selectedSpec.binary }).Count -eq 1
} else {
    $provenance.build.binary_target -ceq [string]$selectedSpec.binary
}
if ($provenance.schema_version -ne 1 -or
    $provenance.format -cne $expectedManifestFormat -or
    (-not $isHostPackage -and $provenance.build.role -cne [string]$selectedSpec.role) -or
    $provenance.build.package_manifest -cne [string]$selectedSpec.manifest -or
    -not $selectedTargetValid -or
    $provenance.build.profile -cne 'release' -or
    [string]$provenance.build.package_version -cnotmatch '\A[0-9A-Za-z.+-]{1,128}\z') {
    throw 'Frontend build manifest package, binary, role, version, or release profile is not an approved exact coordinate.'
}
Assert-SourceIdentity $provenance.source 'Build source' $isHostPackage
if ($null -ne $provenance.source['package_manifest_path'] -and
    [string]$provenance.source.package_manifest_path -cne [string]$provenance.build.package_manifest) {
    throw 'Frontend selected package-manifest path differs between build and source provenance.'
}
Assert-DependencyPins $provenance 'Build manifest'
Assert-CompatibilityManifest $provenance $packageName $selectedSpec 'Build manifest'
$provenanceResources = Assert-ResourceManifest $provenance $selectedSpec 'Build manifest'
Assert-InstalledResourceFiles $packagePath $selectedSpec $provenanceResources 'Frontend package'
if ([string]$provenance.toolchain.pinned_channel -notmatch '\A[A-Za-z0-9._-]{1,128}\z' -or
    [string]::IsNullOrWhiteSpace([string]$provenance.toolchain.rustc_version_verbose) -or
    [string]::IsNullOrWhiteSpace([string]$provenance.toolchain.cargo_version_verbose)) {
    throw 'Frontend build manifest is missing the pinned Rust toolchain evidence.'
}
$expectedSiblings = @($selectedSpec.required_siblings)
$recordedSiblings = @(
    @($provenance.compatibility.required_sibling_binaries | ForEach-Object { [string]$_ })
)
if ($expectedSiblings.Count -ne $recordedSiblings.Count) {
    throw 'Frontend build manifest compatibility requirements do not match the installed CLI contract.'
}
for ($index = 0; $index -lt $expectedSiblings.Count; $index++) {
    if ($recordedSiblings[$index] -cne [string]$expectedSiblings[$index]) {
        throw 'Frontend build manifest compatibility requirements do not match the installed CLI contract.'
    }
}

$artifactRows = @($provenance.artifacts | Where-Object {
    [string]$_.target_name -ceq [string]$selectedSpec.binary -and
    [string]$_.file -ceq ("bin/" + [string]$selectedSpec.binary + '.exe')
})
if ($artifactRows.Count -ne 1 -or (-not $isHostPackage -and @($provenance.artifacts).Count -ne 1)) {
    throw 'Build manifest must identify exactly one approved selected binary artifact.'
}
$binaryLeaf = [string]$selectedSpec.binary + '.exe'
$packageBinary = [IO.Path]::GetFullPath((Join-Path (Join-Path $packagePath 'bin') $binaryLeaf))
$expectedPackageBinary = [IO.Path]::GetFullPath((Join-Path (Join-Path $packagePath 'bin') ([string]$artifactRows[0].target_name + '.exe')))
if (-not $packageBinary.Equals($expectedPackageBinary, [StringComparison]::OrdinalIgnoreCase) -or
    -not (Test-Path -LiteralPath $packageBinary -PathType Leaf)) {
    throw "Frontend package is missing the exact $binaryLeaf selected by its build manifest."
}
Assert-NoReparseTraversal $packageBinary
$binaryItem = Get-Item -LiteralPath $packageBinary -Force
if ($binaryItem.Length -le 0 -or $binaryItem.Length -gt 1073741824) { throw 'Frontend executable is empty or exceeds the 1 GiB installer limit.' }
$executableHash = [string]$artifactRows[0].artifact_sha256
$sourceHash = [string]$artifactRows[0].source_sha256
Assert-Hash $executableHash 'artifacts.artifact_sha256'
Assert-Hash $sourceHash 'artifacts.source_sha256'
if ($sourceHash -cne $executableHash -or
    [long]$artifactRows[0].bytes -ne $binaryItem.Length -or
    (Get-Sha256 $packageBinary) -cne $executableHash) {
    throw 'Frontend package executable size or SHA-256 does not match the exact source/build manifest.'
}
$buildManifestHash = Get-Sha256 $manifestPath

$existing = @{}
foreach ($name in $packageMap.Keys) {
    $existing[$name] = Get-InstalledFrontend $name $installRoot $packageMap[$name] $packageName
    if ($null -ne $existing[$name] -and $name -cne $packageName) {
        $installed = $existing[$name]
        Assert-HostIpcCompatible $provenance 'Selected package' $installed.build_manifest "Installed sibling '$name' build manifest"
    }
}

$binaryDestination = Join-Path $installRoot $binaryLeaf
$buildDestination = Join-Path $installRoot ([string]$selectedSpec.binary + '.build-manifest.json')
$receiptDestination = Join-Path $installRoot ([string]$selectedSpec.binary + '.install-receipt.json')
# Retired: no server staging directory is ever created and no npm install
# ever runs. A legacy server manifest never reaches this point: the
# new-package gate above (Assert-ResourceManifest without the legacy switch)
# rejects it. Only the Antigravity bridge groups below are planned.
$resourceDestinationRoot = $null
$additionalResourceGroups = @()
if ($null -ne $provenanceResources -and $provenanceResources.Contains('additional_groups')) {
    $additionalResourceGroups = @($provenanceResources['additional_groups'] | Where-Object { $null -ne $_ })
}
$additionalResourcePlans = @(
    foreach ($group in $additionalResourceGroups) {
        [ordered]@{
            coordinate = [string]$group.coordinate
            group = $group
            destination = [IO.Path]::GetFullPath((Join-Path $installRoot ([string]$group.installed_relative_root)))
            stage_root = $null
            moved = $false
            parent_created = $false
        }
    }
)
if ($packageName -ceq 'eliot-swarm-controller') {
    $gateway = $existing['swarm-gateway']
    if ($null -ne $gateway) {
        Assert-GatewayLauncherCompatible $provenance $gateway.build_manifest
    }
}
$hostManifest = if ($packageName -ceq 'eliot-swarm-controller') { $provenance } elseif ($null -ne $existing['eliot-swarm-controller']) { $existing['eliot-swarm-controller'].build_manifest } else { $null }
$gatewayManifest = if ($packageName -ceq 'swarm-gateway') { $provenance } elseif ($null -ne $existing['swarm-gateway']) { $existing['swarm-gateway'].build_manifest } else { $null }
if ($null -ne $hostManifest -and $null -ne $gatewayManifest) {
    Assert-GatewayLauncherCompatible $hostManifest $gatewayManifest
}
$requiredQueue = [Collections.Generic.Queue[string]]::new()
$visitedRequirements = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
[void]$visitedRequirements.Add($packageName)
foreach ($requiredName in $expectedSiblings) {
    $requiredQueue.Enqueue([string]$requiredName)
}
while ($requiredQueue.Count -gt 0) {
    $requiredName = $requiredQueue.Dequeue()
    if (-not $packageMap.Contains($requiredName)) {
        throw "Package '$packageName' has an unsupported sibling package requirement '$requiredName'."
    }
    if ($requiredName -ceq $packageName) {
        throw "Package '$packageName' has a circular sibling requirement."
    }
    if (-not $visitedRequirements.Add($requiredName)) { continue }

    $requiredPackage = $existing[$requiredName]
    if ($null -eq $requiredPackage) {
        throw "Required sibling package '$requiredName' is not installed with verified provenance. Install that exact package to '$installRoot' first."
    }
    $requiredBinary = [string]$packageMap[$requiredName].binary + '.exe'
    if (-not (Test-Path -LiteralPath (Join-Path $installRoot $requiredBinary) -PathType Leaf)) {
        throw "Required sibling '$requiredBinary' is missing from '$installRoot'."
    }
    foreach ($transitiveName in @($packageMap[$requiredName].required_siblings)) {
        $requiredQueue.Enqueue([string]$transitiveName)
    }
}
if ($null -ne $existing[$packageName]) {
    $installed = $existing[$packageName]
    if ($installed.source_commit -cne [string]$provenance.source.commit -or
        $installed.source_tree -cne [string]$provenance.source.tree -or
        $installed.executable_sha256 -cne $executableHash -or
        $installed.build_manifest_sha256 -cne $buildManifestHash) {
        throw "A different '$packageName' artifact is already installed; this installer never overwrites it. Use a new empty install directory."
    }
    $status = 'unchanged'
} else {
    if ($PSCmdlet.ShouldProcess($binaryDestination, "Create-only install of $packageName frontend executable and exact provenance sidecars")) {
        $nonce = [guid]::NewGuid().ToString('N')
        $stageBinary = Join-Path $installRoot ('.' + $binaryLeaf + '.' + $nonce + '.stage')
        $stageBuild = Join-Path $installRoot ('.' + [string]$selectedSpec.binary + '.build-manifest.' + $nonce + '.stage')
        $stageReceipt = Join-Path $installRoot ('.' + [string]$selectedSpec.binary + '.install-receipt.' + $nonce + '.stage')
        foreach ($plan in $additionalResourcePlans) {
            $safeCoordinate = [string]$plan.coordinate -replace '[^A-Za-z0-9._-]', '_'
            $plan.stage_root = Join-Path $installRoot ('.resources.' + $safeCoordinate + '.' + $nonce + '.stage')
        }
        $createdDestinations = [Collections.Generic.List[string]]::new()
        $receiptHash = $null
        try {
            Copy-ToStage $packageBinary $stageBinary
            Copy-ToStage $manifestPath $stageBuild
            foreach ($plan in $additionalResourcePlans) {
                [void][IO.Directory]::CreateDirectory([string]$plan.stage_root)
                foreach ($row in @($plan.group.files)) {
                    $stageResourcePath = Join-Path ([string]$plan.stage_root) ([string]$row.path)
                    [void][IO.Directory]::CreateDirectory((Split-Path -Parent $stageResourcePath))
                    $packageResourcePath = Join-Path $packagePath ([string]$row.file -replace '/', [IO.Path]::DirectorySeparatorChar)
                    Copy-ToStage $packageResourcePath $stageResourcePath
                    $stageResourceItem = Get-Item -LiteralPath $stageResourcePath -Force
                    if ([long]$stageResourceItem.Length -ne [long]$row.bytes -or
                        (Get-Sha256 $stageResourcePath) -cne [string]$row.artifact_sha256) {
                        throw "Staged additional resource '$($row.path)' failed its pinned size or SHA-256 check."
                    }
                }
            }
            $packageManifestHash = [string]$provenance.source.package_manifest_sha256
            $workspaceManifestHash = if ($null -ne $provenance.source['workspace_cargo_toml_sha256']) {
                [string]$provenance.source.workspace_cargo_toml_sha256
            } else {
                [string]$provenance.source.cargo_toml_sha256
            }
            $receipt = [ordered]@{
                schema_version = 1
                format = 'eliot.frontend_install_receipt.v1'
                role = [string]$selectedSpec.role
                package_name = $packageName
                package_version = [string]$provenance.build.package_version
                package_manifest = [string]$selectedSpec.manifest
                binary_target = [string]$selectedSpec.binary
                required_sibling_binaries = @($expectedSiblings)
                binary_file = $binaryLeaf
                executable_sha256 = $executableHash
                build_manifest_file = [IO.Path]::GetFileName($buildDestination)
                build_manifest_sha256 = $buildManifestHash
                package_manifest_sha256 = $packageManifestHash
                workspace_cargo_toml_sha256 = $workspaceManifestHash
                source_commit = [string]$provenance.source.commit
                source_tree = [string]$provenance.source.tree
                cargo_toml_sha256 = [string]$provenance.source.cargo_toml_sha256
                cargo_lock_sha256 = [string]$provenance.source.cargo_lock_sha256
                rust_toolchain_toml_sha256 = [string]$provenance.source.rust_toolchain_toml_sha256
                resources = if ($null -ne $provenanceResources) {
                    [ordered]@{
                        additional_groups = @(
                            foreach ($group in $additionalResourceGroups) {
                                [ordered]@{
                                    coordinate = [string]$group.coordinate
                                    module_id = [string]$group.module_id
                                    artifact_id = [string]$group.artifact_id
                                    version = [string]$group.version
                                    repository_relative_root = [string]$group.repository_relative_root
                                    installed_relative_root = [string]$group.installed_relative_root
                                    files = @(
                                        foreach ($row in @($group.files)) {
                                            [ordered]@{
                                                path = [string]$row.path
                                                file = [string]$row.file
                                                artifact_sha256 = [string]$row.artifact_sha256
                                            }
                                        }
                                    )
                                }
                            }
                        )
                    }
                } else { $null }
            }
            $receiptJson = ConvertTo-Json -InputObject $receipt -Depth 6
            $receiptBytes = [Text.UTF8Encoding]::new($false).GetBytes($receiptJson + [Environment]::NewLine)
            $receiptStream = [IO.File]::Open($stageReceipt, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
            try {
                $receiptStream.Write($receiptBytes, 0, $receiptBytes.Length)
                $receiptStream.Flush($true)
            } finally { $receiptStream.Dispose() }
            $receiptHash = Get-Sha256 $stageReceipt
            if ((Get-Sha256 $stageBinary) -cne $executableHash -or
                (Get-Sha256 $stageBuild) -cne $buildManifestHash) {
                throw 'Staged frontend executable or build manifest failed its pinned SHA-256 check.'
            }
            $installDestinations = @($binaryDestination, $buildDestination, $receiptDestination)
            foreach ($plan in $additionalResourcePlans) { $installDestinations += [string]$plan.destination }
            foreach ($destination in $installDestinations) {
                Assert-NoReparseTraversal $destination
                if (Test-Path -LiteralPath $destination) {
                    throw "Install destination appeared during create-only installation: $destination"
                }
            }
            [IO.File]::Move($stageBinary, $binaryDestination)
            $createdDestinations.Add($binaryDestination)
            [IO.File]::Move($stageBuild, $buildDestination)
            $createdDestinations.Add($buildDestination)
            [IO.File]::Move($stageReceipt, $receiptDestination)
            $createdDestinations.Add($receiptDestination)
            foreach ($plan in $additionalResourcePlans) {
                $additionalParent = Split-Path -Parent ([string]$plan.destination)
                Assert-NoReparseTraversal $additionalParent
                if (-not (Test-Path -LiteralPath $additionalParent -PathType Container)) {
                    [void][IO.Directory]::CreateDirectory($additionalParent)
                    $plan.parent_created = $true
                }
                Assert-NoReparseTraversal $additionalParent
                [IO.Directory]::Move([string]$plan.stage_root, [string]$plan.destination)
                $plan.moved = $true
            }
            foreach ($destination in $installDestinations) {
                Assert-NoReparseTraversal $destination
            }
            if ((Get-Sha256 $binaryDestination) -cne $executableHash -or
                (Get-Sha256 $buildDestination) -cne $buildManifestHash) {
                throw 'Installed frontend bytes differ from the staged package pins.'
            }
            Assert-InstalledResourceFiles $installRoot $selectedSpec $provenanceResources 'Installed frontend'
            $status = 'installed'
        } catch {
            $installFailure = $_
            $cleanupFailures = [Collections.Generic.List[string]]::new()
            $cleanupHashes = @{}
            $cleanupHashes[$binaryDestination] = $executableHash
            $cleanupHashes[$buildDestination] = $buildManifestHash
            $cleanupHashes[$receiptDestination] = $receiptHash
            $movedAdditionalResources = @($additionalResourcePlans | Where-Object { $_.moved -eq $true })
            if ($movedAdditionalResources.Count -gt 0) {
                try {
                    foreach ($plan in $movedAdditionalResources) {
                        Assert-NoReparseTraversal ([string]$plan.destination)
                        Assert-InstalledResourceRows -ResourceRoot ([string]$plan.destination) -InstallRoot $installRoot -Rows @($plan.group.files) -Label "Rollback $($plan.coordinate) resource check"
                    }
                    foreach ($plan in $movedAdditionalResources) {
                        Remove-Item -LiteralPath ([string]$plan.destination) -Recurse -Force -Confirm:$false
                        $plan.moved = $false
                    }
                } catch {
                    $cleanupFailures.Add("could not roll back pinned resource roots: $($_.Exception.Message)")
                }
            }
            for ($index = $createdDestinations.Count - 1; $index -ge 0; $index--) {
                $createdPath = $createdDestinations[$index]
                try {
                    if (Test-Path -LiteralPath $createdPath -PathType Leaf) {
                        if ((Get-Sha256 $createdPath) -ceq [string]$cleanupHashes[$createdPath]) {
                            Remove-Item -LiteralPath $createdPath -Force -Confirm:$false
                        } else {
                            $cleanupFailures.Add("left changed destination untouched: $createdPath")
                        }
                    }
                } catch {
                    $cleanupFailures.Add("could not roll back ${createdPath}: $($_.Exception.Message)")
                }
            }
            foreach ($plan in $additionalResourcePlans | Where-Object { $_.parent_created -eq $true }) {
                $additionalParent = Split-Path -Parent ([string]$plan.destination)
                try {
                    if ((Test-Path -LiteralPath $additionalParent -PathType Container) -and
                        @((Get-ChildItem -LiteralPath $additionalParent -Force)).Count -eq 0) {
                        Remove-Item -LiteralPath $additionalParent -Force -Confirm:$false
                    }
                } catch {
                    $cleanupFailures.Add("could not roll back additional resource parent ${additionalParent}: $($_.Exception.Message)")
                }
            }
            if ($cleanupFailures.Count -gt 0) {
                throw "Create-only install failed: $($installFailure.Exception.Message) Rollback notes: $($cleanupFailures -join '; '). Inspect those exact paths before retrying."
            }
            throw $installFailure
        } finally {
            foreach ($stage in @($stageBinary, $stageBuild, $stageReceipt)) {
                if (Test-Path -LiteralPath $stage -PathType Leaf) { Remove-Item -LiteralPath $stage -Force -Confirm:$false }
            }
            foreach ($plan in $additionalResourcePlans) {
                if ($null -ne $plan.stage_root -and (Test-Path -LiteralPath ([string]$plan.stage_root))) {
                    Remove-Item -LiteralPath ([string]$plan.stage_root) -Recurse -Force -Confirm:$false
                }
            }
        }
    } else {
        $status = 'what_if'
    }
}

[pscustomobject]@{
    status = $status
    role = [string]$selectedSpec.role
    package = $packageName
    executable = [IO.Path]::GetFullPath($binaryDestination)
    executable_sha256 = $executableHash
    build_manifest = [IO.Path]::GetFullPath($buildDestination)
    build_manifest_sha256 = $buildManifestHash
    install_receipt = [IO.Path]::GetFullPath($receiptDestination)
    resource_root = $resourceDestinationRoot
    additional_resource_roots = @($additionalResourcePlans | ForEach-Object { [string]$_.destination })
    source_commit = [string]$provenance.source.commit
    source_tree = [string]$provenance.source.tree
}
