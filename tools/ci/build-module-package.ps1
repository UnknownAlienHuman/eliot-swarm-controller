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

function Get-RootGatewayLauncherArguments([object[]] $Targets) {
    $matches = @($Targets | Where-Object { [string]$_.name -ceq 'swarm' -and @($_.kind) -contains 'bin' })
    if ($matches.Count -ne 1) { throw 'Root host package must expose exactly one swarm binary for gateway compatibility.' }
    $source = Get-Content -LiteralPath ([string]$matches[0].src_path) -Raw
    $start = $source.IndexOf('async fn run_gateway_binary', [StringComparison]::Ordinal)
    if ($start -lt 0) { throw 'Root swarm binary has no run_gateway_binary compatibility launcher.' }
    $launcher = $source.Substring($start)
    foreach ($literal in @('"swarm-gateway.exe"', '"swarm-gateway"', 'child.arg("--config").arg(path)', 'child.arg("--data-dir").arg(path)')) {
        if (-not $launcher.Contains($literal)) { throw "Root gateway launcher no longer satisfies the sibling CLI contract: missing $literal" }
    }
    return @('--config', '--data-dir')
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
        -not $policyNames.Add([string]$coordinate.package_name)) {
        throw 'Module package policy has a malformed or repeated package/manifest/binary coordinate.'
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
$compatibility = $null
if ($Package -ceq 'eliot-swarm-controller') {
    $hostIpcProtocolVersion = Get-HostIpcProtocolVersion $repoRoot
    $rustcHostTriple = Get-RustcHostTriple $rustcVersion
    $gatewayArguments = Get-RootGatewayLauncherArguments @($selected.targets)
    $compatibility = [ordered]@{
        required_sibling_binaries = @()
        host_ipc = [ordered]@{ protocol_version = $hostIpcProtocolVersion }
        target = [ordered]@{ rustc_host_triple = $rustcHostTriple }
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
    installation = [ordered]@{
        descriptor_generated = $false
        installed = $false
        registered = $false
        route_enabled = $false
        activated = $false
    }
}
if ($null -ne $compatibility) { $buildManifest['compatibility'] = $compatibility }
$json = ConvertTo-Json -InputObject $buildManifest -Depth 12
$manifestPath = Join-Path $outputPath 'build-manifest.json'
[IO.File]::WriteAllText($manifestPath, $json + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
Write-Output "Packaged $($binaryRows.Count) selected executable(s) and build-manifest.json."
