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
if ($policy.schema_version -ne 1 -or $policy.format -cne 'eliot.module_build_policy.v1') {
    throw 'Unsupported module package policy format.'
}
if (@($policy.approved_package_names | Where-Object { [string]$_ -ceq $Package }).Count -ne 1) {
    throw "Package '$Package' is not on the explicit module-package allowlist."
}

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
        'metadata', '--locked', '--no-deps', '--format-version', '1'
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
if (-not (Test-PathWithin $manifestPath $repoRoot)) {
    throw 'The selected package manifest is outside the source checkout.'
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

$binTargets = @($selected.targets | Where-Object { @($_.kind) -contains 'bin' })
if ($binTargets.Count -eq 0) { throw "Package '$Package' has no actual Cargo binary target." }
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
    '--package', $Package, '--bins', '--locked',
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
    artifacts = @($binaryRows)
    installation = [ordered]@{
        descriptor_generated = $false
        installed = $false
        registered = $false
        route_enabled = $false
        activated = $false
    }
}
$json = ConvertTo-Json -InputObject $buildManifest -Depth 12
$manifestPath = Join-Path $outputPath 'build-manifest.json'
[IO.File]::WriteAllText($manifestPath, $json + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
Write-Output "Packaged $($binaryRows.Count) selected executable(s) and build-manifest.json."
