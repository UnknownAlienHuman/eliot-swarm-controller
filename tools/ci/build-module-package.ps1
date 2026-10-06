[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string] $Package,
    [string] $Profile = 'iterate',
    [Parameter(Mandatory = $true)][string] $TargetDir,
    [Parameter(Mandatory = $true)][string] $OutputDir
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (-not $IsWindows) { throw 'This packaging script emits Windows executables only.' }

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
$rootManifest = Join-Path $repoRoot 'Cargo.toml'
$lockFile = Join-Path $repoRoot 'Cargo.lock'
$toolchainFile = Join-Path $repoRoot 'rust-toolchain.toml'
$policyFile = Join-Path $PSScriptRoot 'module-package-policy.json'

function Get-CanonicalPath([string] $Path, [string] $Label) {
    if (-not [IO.Path]::IsPathFullyQualified($Path)) { throw "$Label must be an absolute path." }
    return [IO.Path]::GetFullPath($Path)
}

function Test-PathWithin([string] $Path, [string] $Root) {
    $pathFull = [IO.Path]::GetFullPath($Path)
    $rootFull = [IO.Path]::GetFullPath($Root).TrimEnd([char[]]@(
        [IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar
    ))
    $comparison = [StringComparison]::OrdinalIgnoreCase
    if ($pathFull.Equals($rootFull, $comparison)) { return $true }
    return $pathFull.StartsWith($rootFull + [IO.Path]::DirectorySeparatorChar, $comparison)
}

function Invoke-NativeText([string] $File, [string[]] $Arguments) {
    $captured = & $File @Arguments 2>&1
    $exitCode = $LASTEXITCODE
    $text = (@($captured) | ForEach-Object { [string]$_ }) -join [Environment]::NewLine
    if ($exitCode -ne 0) {
        throw ($File + ' failed with exit code ' + $exitCode + [Environment]::NewLine + $text)
    }
    return $text.Trim()
}

function Assert-CleanCheckout([string] $Root, [string] $When) {
    $status = Invoke-NativeText 'git' @(
        '-C', $Root, 'status', '--porcelain=v1', '--untracked-files=all'
    )
    if (-not [string]::IsNullOrWhiteSpace($status)) {
        throw "The checkout must be clean $When; refusing to label a build with only HEAD."
    }
}

function Get-Sha256([string] $Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

. (Join-Path $PSScriptRoot 'SwarmBuildProvenanceHelpers.ps1')

function Get-HostGatewayLauncherArguments([object] $Target) {
    if ($null -eq $Target -or [string]$Target.name -cne 'swarm-kernel-host' -or
        @($Target.kind) -notcontains 'bin') {
        throw 'The durable host package must expose exactly one swarm-kernel-host binary for gateway compatibility.'
    }
    $source = Get-Content -LiteralPath ([string]$Target.src_path) -Raw
    $start = $source.IndexOf('async fn run_gateway_binary', [StringComparison]::Ordinal)
    if ($start -lt 0) { throw 'The swarm-kernel-host binary has no run_gateway_binary compatibility launcher.' }
    $launcher = $source.Substring($start)
    foreach ($literal in @('"swarm-gateway.exe"', '"swarm-gateway"', 'child.arg("--config").arg(path)', 'child.arg("--data-dir").arg(path)')) {
        if (-not $launcher.Contains($literal)) { throw "The durable host gateway launcher no longer satisfies the sibling CLI contract: missing $literal" }
    }
    return @('--config', '--data-dir')
}

function Get-KernelHostTarget([object] $Metadata, [object[]] $MemberIds) {
    $matches = @($Metadata.packages | ForEach-Object {
        $package = $_
        if ([string]$package.name -ceq 'swarm-kernel-host' -and
            @($MemberIds | Where-Object { [string]$_ -ceq [string]$package.id }).Count -eq 1) {
            $package
        }
    })
    if ($matches.Count -ne 1) {
        throw 'The workspace must contain exactly one member named swarm-kernel-host before host provenance can be packaged.'
    }
    $targets = @($matches[0].targets | Where-Object {
        [string]$_.name -ceq 'swarm-kernel-host' -and @($_.kind) -contains 'bin'
    })
    if ($targets.Count -ne 1) {
        throw 'The swarm-kernel-host workspace member must expose exactly one swarm-kernel-host binary target.'
    }
    return $targets[0]
}

function Get-SupervisorTarget([object] $Metadata, [object[]] $MemberIds) {
    $matches = @($Metadata.packages | ForEach-Object {
        $package = $_
        if ([string]$package.name -ceq 'swarm-supervisor' -and
            @($MemberIds | Where-Object { [string]$_ -ceq [string]$package.id }).Count -eq 1) {
            $package
        }
    })
    if ($matches.Count -ne 1) {
        throw 'The workspace must contain exactly one member named swarm-supervisor before host provenance can be packaged.'
    }
    $targets = @($matches[0].targets | Where-Object {
        [string]$_.name -ceq 'swarm-supervisor' -and @($_.kind) -contains 'bin'
    })
    if ($targets.Count -ne 1) {
        throw 'The swarm-supervisor workspace member must expose exactly one swarm-supervisor binary target.'
    }
    return $targets[0]
}

function Get-PolicySiblingBinaries([object] $Coordinate) {
    if ($null -eq $Coordinate.required_sibling_binaries) { return @() }
    return @($Coordinate.required_sibling_binaries | ForEach-Object { [string]$_ })
}

function Assert-PolicyResourceCoordinate([object] $Coordinate) {
    if ($null -eq $Coordinate.resource_coordinate) {
        foreach ($field in @('resource_installed_relative_root', 'resource_repository_relative_root', 'resource_files')) {
            if ($null -ne $Coordinate[$field]) {
                throw "Package '$($Coordinate.package_name)' has resource metadata without a resource coordinate."
            }
        }
        return
    }
    if ([string]$Coordinate.resource_coordinate -cne 'swarm-kernel-host-opencode-resources' -or
        [string]$Coordinate.resource_installed_relative_root -cne 'resources/modules/opencode' -or
        [string]$Coordinate.resource_repository_relative_root -cne 'modules/opencode' -or
        $Coordinate.resource_files -isnot [array]) {
        throw "Package '$($Coordinate.package_name)' has an unsupported resource coordinate."
    }
    $expected = @(
        @{ path = 'serve.mjs'; role = 'server_program' },
        @{ path = 'native-mcp-proof.mjs'; role = 'plugin_module' },
        @{ path = 'index.mjs'; role = 'plugin_entry' },
        @{ path = 'package.json'; role = 'dependency_manifest' },
        @{ path = 'package-lock.json'; role = 'dependency_lock' }
    )
    $actual = @($Coordinate.resource_files)
    if ($actual.Count -ne $expected.Count) { throw "Package '$($Coordinate.package_name)' has an incomplete resource file coordinate." }
    for ($index = 0; $index -lt $expected.Count; $index++) {
        if ($actual[$index] -isnot [System.Collections.IDictionary] -or
            [string]$actual[$index].path -cne $expected[$index].path -or
            [string]$actual[$index].role -cne $expected[$index].role) {
            throw "Package '$($Coordinate.package_name)' resource file coordinate drifted from the pinned OpenCode set."
        }
    }
}

function Get-KernelHostResourcePins([string] $PackageName, [object] $Coordinate) {
    if ($PackageName -cne 'swarm-kernel-host') { return @() }
    Assert-PolicyResourceCoordinate $Coordinate
    $resourceRoot = Join-Path $repoRoot 'modules/opencode'
    $pins = @(
        foreach ($entry in @($Coordinate.resource_files)) {
            $sourcePath = [IO.Path]::GetFullPath((Join-Path $resourceRoot ([string]$entry.path)))
            if (-not (Test-PathWithin $sourcePath $resourceRoot) -or
                -not (Test-Path -LiteralPath $sourcePath -PathType Leaf)) {
                throw "Pinned kernel resource is missing from the repository: $sourcePath"
            }
            $item = Get-Item -LiteralPath $sourcePath -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or $item.Length -le 0) {
                throw "Pinned kernel resource is not a regular non-empty file: $sourcePath"
            }
            [ordered]@{
                path = [string]$entry.path
                role = [string]$entry.role
                file = 'resources/modules/opencode/' + [string]$entry.path
                bytes = [long]$item.Length
                source_sha256 = Get-Sha256 $sourcePath
            }
        }
    )
    return $pins
}

if (-not (Test-Path -LiteralPath $rootManifest -PathType Leaf) -or
    -not (Test-Path -LiteralPath $lockFile -PathType Leaf) -or
    -not (Test-Path -LiteralPath $toolchainFile -PathType Leaf)) {
    throw 'Expected workspace manifest, lockfile, or pinned toolchain file is missing.'
}

$targetPath = Get-CanonicalPath $TargetDir 'TargetDir'
$outputPath = Get-CanonicalPath $OutputDir 'OutputDir'
if ((Test-PathWithin $targetPath $repoRoot) -or (Test-PathWithin $repoRoot $targetPath)) {
    throw 'TargetDir must be outside and disjoint from the source checkout.'
}
if ((Test-PathWithin $outputPath $repoRoot) -or (Test-PathWithin $repoRoot $outputPath)) {
    throw 'OutputDir must be outside and disjoint from the source checkout.'
}
if ((Test-PathWithin $targetPath $outputPath) -or (Test-PathWithin $outputPath $targetPath)) {
    throw 'TargetDir and OutputDir must be separate, non-overlapping directories.'
}
if (Test-Path -LiteralPath $targetPath -PathType Leaf) { throw 'TargetDir names a file.' }
if (Test-Path -LiteralPath $outputPath) {
    throw 'OutputDir must not already exist; refusing to reuse or overwrite a package artifact.'
}

$allowedProfiles = @('iterate', 'release')
if (@($allowedProfiles | Where-Object { $_ -ceq $Profile }).Count -ne 1) {
    throw "Profile must be one of: $($allowedProfiles -join ', ')."
}
$rootCargoText = Get-Content -LiteralPath $rootManifest -Raw
$profileHeader = "[profile.$Profile]"
if ($rootCargoText -notmatch [regex]::Escape($profileHeader)) {
    throw "Profile '$Profile' is not explicitly defined in the root Cargo.toml."
}

$toolchainText = Get-Content -LiteralPath $toolchainFile -Raw
$channelMatch = [regex]::Match($toolchainText, '(?m)^\s*channel\s*=\s*"([^"]+)"\s*$')
if (-not $channelMatch.Success) { throw 'Could not resolve a channel from rust-toolchain.toml.' }
$toolchainChannel = $channelMatch.Groups[1].Value

$policy = Get-Content -LiteralPath $policyFile -Raw | ConvertFrom-Json -AsHashtable
if ($policy.schema_version -ne 2 -or $policy.format -cne 'eliot.module_build_policy.v2' -or
    $policy.approved_package_coordinates -isnot [array] -or $policy.approved_package_coordinates.Count -eq 0) {
    throw 'Unsupported module package policy format.'
}

$policyNames = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
foreach ($coordinate in $policy.approved_package_coordinates) {
    if ($coordinate -isnot [System.Collections.IDictionary] -or
        [string]$coordinate.package_name -cnotmatch '\A[A-Za-z0-9][A-Za-z0-9_-]{0,127}\z' -or
        [string]$coordinate.manifest -cnotmatch '\A[A-Za-z0-9._/-]{1,512}\z' -or
        [string]$coordinate.manifest -match '(^|/)\.\.(/|$)' -or
        [string]$coordinate.binary_target -cnotmatch '\A[A-Za-z0-9][A-Za-z0-9_-]{0,99}\z' -or
        ($null -ne $coordinate.role -and [string]$coordinate.role -cnotmatch '\A[A-Za-z0-9][A-Za-z0-9_-]{0,63}\z') -or
        ($null -ne $coordinate.required_sibling_binaries -and
            $coordinate.required_sibling_binaries -isnot [array]) -or
        -not $policyNames.Add([string]$coordinate.package_name)) {
        throw 'Module package policy has a malformed or repeated package/manifest/binary coordinate.'
    }
    Assert-PolicyResourceCoordinate $coordinate
    $siblingNames = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($sibling in @(Get-PolicySiblingBinaries $coordinate)) {
        if ($sibling -cnotmatch '\A[A-Za-z0-9][A-Za-z0-9_-]{0,127}\z' -or
            -not $siblingNames.Add($sibling)) {
            throw "Package '$($coordinate.package_name)' repeats or malformed required sibling coordinate."
        }
    }
}
$selectedCoordinates = @(
    $policy.approved_package_coordinates | Where-Object {
        [string]$_.package_name -ceq $Package
    }
)
if ($selectedCoordinates.Count -ne 1) {
    throw "Package '$Package' is not on the explicit module-package allowlist."
}
$selectedCoordinate = $selectedCoordinates[0]

Push-Location $repoRoot
try {
    $activeToolchain = Invoke-NativeText 'rustup' @('show', 'active-toolchain')
    $activeToolchainName = ($activeToolchain -split '\s+', 2)[0]
    if ($activeToolchainName -cne $toolchainChannel -and
        -not $activeToolchainName.StartsWith($toolchainChannel + '-', [StringComparison]::Ordinal)) {
        throw "Active toolchain '$activeToolchainName' does not match rust-toolchain.toml channel '$toolchainChannel'."
    }
    $rustcVersion = Invoke-NativeText 'rustc' @('--version', '--verbose')
    $cargoVersion = Invoke-NativeText 'cargo' @('--version', '--verbose')
} finally {
    Pop-Location
}

Assert-CleanCheckout $repoRoot 'before build'
$commit = Invoke-NativeText 'git' @('-C', $repoRoot, 'rev-parse', '--verify', 'HEAD')
$tree = Invoke-NativeText 'git' @('-C', $repoRoot, 'rev-parse', '--verify', 'HEAD^{tree}')
$reportedRoot = Invoke-NativeText 'git' @('-C', $repoRoot, 'rev-parse', '--show-toplevel')
if (-not ([IO.Path]::GetFullPath($reportedRoot)).Equals(
    $repoRoot, [StringComparison]::OrdinalIgnoreCase
)) { throw 'Selected source directory is not the Git checkout root.' }

Push-Location $repoRoot
try {
    $metadataText = Invoke-NativeText 'cargo' @(
        'metadata', '--locked', '--format-version', '1'
    )
} finally {
    Pop-Location
}
$metadata = $metadataText | ConvertFrom-Json -AsHashtable
$memberIds = @($metadata.workspace_members)
$selectedMatches = @(
    $metadata.packages | Where-Object {
        $candidateId = [string]$_.id
        ([string]$_.name -ceq $Package) -and
        (@($memberIds | Where-Object { [string]$_ -ceq $candidateId }).Count -eq 1)
    }
)
if ($selectedMatches.Count -ne 1) {
    throw "Package '$Package' must resolve to exactly one actual Cargo workspace member; found $($selectedMatches.Count)."
}
$selected = $selectedMatches[0]
$manifestPath = [IO.Path]::GetFullPath([string]$selected.manifest_path)
$expectedManifestPath = [IO.Path]::GetFullPath((Join-Path $repoRoot ([string]$selectedCoordinate.manifest)))
if (-not $manifestPath.Equals($expectedManifestPath, [StringComparison]::OrdinalIgnoreCase) -or
    -not (Test-PathWithin $manifestPath $repoRoot)) {
    throw "Package '$Package' resolved to a manifest other than its exact allowlisted coordinate."
}
$vendorRoot = Join-Path $repoRoot 'vendor'
foreach ($target in @($selected.targets)) {
    $sourcePath = [IO.Path]::GetFullPath([string]$target.src_path)
    if (-not (Test-PathWithin $sourcePath $repoRoot)) {
        throw "Target '$($target.name)' is outside the source checkout."
    }
    if ((Test-PathWithin $manifestPath $vendorRoot) -or (Test-PathWithin $sourcePath $vendorRoot)) {
        throw "Package '$Package' has a vendor-backed manifest or target; refusing donor code."
    }
}

$actualBinTargets = @($selected.targets | Where-Object { @($_.kind) -contains 'bin' })
$binTargets = @(
    $actualBinTargets | Where-Object {
        [string]$_.name -ceq [string]$selectedCoordinate.binary_target
    }
)
if ($actualBinTargets.Count -ne 1 -or $binTargets.Count -ne 1) {
    $actualNames = @($actualBinTargets | ForEach-Object { [string]$_.name })
    throw "Package '$Package' target drift: policy names only '$($selectedCoordinate.binary_target)' but Cargo metadata reports [$($actualNames -join ', ')]. Update the explicit coordinate policy before packaging."
}
$dependencyPins = Get-ResolvedDependencyPins $metadata $selected $repoRoot $lockFile
$resourcePins = @(Get-KernelHostResourcePins $Package $selectedCoordinate)
$compatibility = $null
if ($Package -in @('eliot-swarm-controller', 'swarm-kernel-host', 'swarm-supervisor')) {
    $kernelTarget = Get-KernelHostTarget $metadata $memberIds
    $supervisorTarget = Get-SupervisorTarget $metadata $memberIds
    $hostIpcProtocolVersion = Get-HostIpcProtocolVersion $repoRoot
    $rustcHostTriple = Get-RustcHostTriple $rustcVersion
    $gatewayArguments = if ($Package -in @('eliot-swarm-controller', 'swarm-kernel-host')) {
        Get-HostGatewayLauncherArguments $kernelTarget
    } else { @() }
    $hostRuntime = [ordered]@{
        package_name = 'swarm-kernel-host'
        manifest = 'crates/swarm-kernel-host/Cargo.toml'
        binary_target = 'swarm-kernel-host'
        role = 'host_runtime'
        resource_coordinate = 'swarm-kernel-host-opencode-resources'
        resource_installed_relative_root = 'resources/modules/opencode'
    }
    $hostSupervisor = [ordered]@{
        package_name = 'swarm-supervisor'
        manifest = 'crates/swarm-supervisor/Cargo.toml'
        binary_target = 'swarm-supervisor'
        role = 'host_supervisor'
    }
    $compatibility = [ordered]@{
        process_role = [string]$selectedCoordinate.role
        required_sibling_binaries = @(Get-PolicySiblingBinaries $selectedCoordinate)
        host_ipc = [ordered]@{ protocol_version = $hostIpcProtocolVersion }
        target = [ordered]@{ rustc_host_triple = $rustcHostTriple }
        host_runtime = $hostRuntime
        host_supervisor = $hostSupervisor
        gateway_launcher = [ordered]@{
            package_name = 'swarm-gateway'
            binary_target = 'swarm-gateway'
            required_arguments = @($gatewayArguments)
        }
    }
}
foreach ($target in $binTargets) {
    $name = [string]$target.name
    if ($name -notmatch '^[A-Za-z0-9][A-Za-z0-9_-]{0,99}$') {
        throw "Binary target name '$name' is not a safe Windows artifact filename."
    }
}

if (Test-Path -LiteralPath $targetPath -PathType Container) {
    $targetItem = Get-Item -LiteralPath $targetPath
    if (($targetItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw 'TargetDir must not itself be a reparse point.'
    }
} else {
    [void][IO.Directory]::CreateDirectory($targetPath)
}

$buildArguments = @(
    'build', '--manifest-path', $rootManifest,
    '--package', $Package, '--bin', [string]$selectedCoordinate.binary_target, '--locked',
    '--profile', $Profile, '--target-dir', $targetPath
)
Push-Location $repoRoot
try {
    & cargo @buildArguments
    $buildExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
if ($buildExitCode -ne 0) { throw "Selected Cargo build failed with exit code $buildExitCode." }
Assert-CleanCheckout $repoRoot 'after build'

$binarySources = @(
    foreach ($target in $binTargets) {
        $source = Join-Path (Join-Path $targetPath $Profile) ([string]$target.name + '.exe')
        if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
            throw "Cargo reported success but expected '$($target.name).exe' is absent."
        }
        [pscustomobject]@{ target_name = [string]$target.name; source_path = $source }
    }
)

if (Test-Path -LiteralPath $outputPath) {
    throw 'OutputDir appeared during the build; refusing to overwrite it.'
}
[void][IO.Directory]::CreateDirectory($outputPath)
$binaryOutputDir = Join-Path $outputPath 'bin'
[void][IO.Directory]::CreateDirectory($binaryOutputDir)
$resourceRows = @(
    foreach ($resource in $resourcePins) {
        $sourcePath = Join-Path (Join-Path $repoRoot 'modules/opencode') ([string]$resource.path)
        $stagedPath = Join-Path $outputPath ([string]$resource.file -replace '/', [IO.Path]::DirectorySeparatorChar)
        $stagedParent = Split-Path -Parent $stagedPath
        [void][IO.Directory]::CreateDirectory($stagedParent)
        Copy-Item -LiteralPath $sourcePath -Destination $stagedPath
        $artifactHash = Get-Sha256 $stagedPath
        if ($artifactHash -cne [string]$resource.source_sha256) {
            throw "Staged hash mismatch for pinned kernel resource '$($resource.path)'."
        }
        [ordered]@{
            path = [string]$resource.path
            role = [string]$resource.role
            file = [string]$resource.file
            bytes = (Get-Item -LiteralPath $stagedPath).Length
            source_sha256 = [string]$resource.source_sha256
            artifact_sha256 = $artifactHash
        }
    }
)

$binaryRows = @(
    foreach ($binary in $binarySources) {
        $fileName = [string]$binary.target_name + '.exe'
        $stagedPath = Join-Path $binaryOutputDir $fileName
        Copy-Item -LiteralPath $binary.source_path -Destination $stagedPath
        $sourceHash = Get-Sha256 $binary.source_path
        $stagedHash = Get-Sha256 $stagedPath
        if ($sourceHash -cne $stagedHash) { throw "Staged hash mismatch for '$fileName'." }
        [pscustomobject]@{
            target_name = [string]$binary.target_name
            file = "bin/$fileName"
            bytes = (Get-Item -LiteralPath $stagedPath).Length
            source_sha256 = $sourceHash
            artifact_sha256 = $stagedHash
        }
    }
)

$manifestRelative = [IO.Path]::GetRelativePath($repoRoot, $manifestPath).Replace('\', '/')
$buildManifest = [ordered]@{
    schema_version = 1
    format = 'eliot.module_build_manifest.v1'
    source = [ordered]@{
        commit = $commit
        tree = $tree
        checkout_clean_before = $true
        checkout_clean_after = $true
        cargo_toml_sha256 = Get-Sha256 $rootManifest
        cargo_lock_sha256 = Get-Sha256 $lockFile
        rust_toolchain_toml_sha256 = Get-Sha256 $toolchainFile
    }
    toolchain = [ordered]@{
        pinned_channel = $toolchainChannel
        active_toolchain = $activeToolchain
        rustc_version_verbose = $rustcVersion
        cargo_version_verbose = $cargoVersion
    }
    build = [ordered]@{
        profile = $Profile
        package_name = [string]$selected.name
        package_version = [string]$selected.version
        package_manifest = $manifestRelative
        binary_targets = @($binTargets | ForEach-Object { [string]$_.name })
        cargo_arguments = @($buildArguments)
        target_dir = $targetPath
    }
    dependency_pins = @($dependencyPins)
    dependency_graph_scope = [ordered]@{
        source = 'cargo_metadata_workspace_resolve'
        package_edges = 'reachable_from_selected_package_in_workspace_resolved_graph_may_overapprox'
        feature_sets = 'workspace_unified_not_package_specific_build_features'
    }
    artifacts = @($binaryRows)
    resources = if ($resourceRows.Count -gt 0) {
        [ordered]@{
            schema_version = 1
            coordinate = [string]$selectedCoordinate.resource_coordinate
            repository_relative_root = [string]$selectedCoordinate.resource_repository_relative_root
            installed_relative_root = [string]$selectedCoordinate.resource_installed_relative_root
            files = @($resourceRows)
            dependency_policy = [ordered]@{ node_modules = 'external_locked_installation' }
        }
    } else { $null }
    installation = [ordered]@{
        descriptor_generated = $false
        installed = $false
        registered = $false
        route_enabled = $false
        activated = $false
    }
}
if ($null -ne $selectedCoordinate.role) {
    $buildManifest.build['role'] = [string]$selectedCoordinate.role
}
if ($null -ne $compatibility) { $buildManifest['compatibility'] = $compatibility }
$json = ConvertTo-Json -InputObject $buildManifest -Depth 12
$manifestPath = Join-Path $outputPath 'build-manifest.json'
[IO.File]::WriteAllText($manifestPath, $json + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
Write-Output "Packaged $($binaryRows.Count) selected executable(s) and build-manifest.json."
