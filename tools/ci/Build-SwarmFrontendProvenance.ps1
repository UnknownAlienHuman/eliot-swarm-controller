[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [string] $Package,
    [Parameter(Mandatory = $true)] [string] $TargetDir,
    [Parameter(Mandatory = $true)] [string] $OutputDir
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (-not $IsWindows) { throw 'Frontend provenance packaging emits Windows executables only.' }

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
$rootManifest = Join-Path $repoRoot 'Cargo.toml'
$lockFile = Join-Path $repoRoot 'Cargo.lock'
$toolchainFile = Join-Path $repoRoot 'rust-toolchain.toml'
$packageMap = @{
    'swarm-mcp' = [ordered]@{
        manifest = 'crates/swarm-mcp/Cargo.toml'
        binary = 'swarm-mcp'
        role = 'frontend_service'
    }
    'swarm-gateway' = [ordered]@{
        manifest = 'crates/swarm-gateway/Cargo.toml'
        binary = 'swarm-gateway'
        role = 'frontend_service'
    }
    'swarm-cli' = [ordered]@{
        manifest = 'crates/swarm-cli/Cargo.toml'
        binary = 'swarm'
        role = 'cli_client'
    }
}

function Get-PublicCliHostRequirement([object] $Target, [object] $SelectedPackage) {
    $source = Get-Content -LiteralPath ([string]$Target.src_path) -Raw
    foreach ($literal in @('fn command_uses_host(', '"swarm-host.exe"', 'HOST_BINARY_MISSING', '.args(arguments).status()', 'fn command_uses_mcp(', '"swarm-mcp.exe"', 'MCP_BINARY_MISSING')) {
        if (-not $source.Contains($literal)) { throw "Public swarm CLI is missing its explicit host sibling contract: $literal" }
    }
    if ($source.Contains('eliot_swarm_controller::') -or $source.Contains('swarm_store::') -or $source.Contains('swarm_kernel::')) {
        throw 'Public swarm CLI source imports a root controller, Store, or kernel implementation.'
    }
    $forbidden = @($SelectedPackage.dependencies | Where-Object {
        [string]$_.name -match '\A(?:eliot-swarm-controller|swarm-store|swarm-kernel|swarm-adapter-)'
    })
    if ($forbidden.Count -gt 0) { throw 'Public swarm CLI package directly depends on root host, Store, kernel, or adapter packages.' }
    return @('eliot-swarm-controller')
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
            throw "Reparse points are not accepted in package paths: $cursor"
        }
    }
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
    $status = Invoke-NativeText 'git' @('-C', $Root, 'status', '--porcelain=v1', '--untracked-files=all')
    if (-not [string]::IsNullOrWhiteSpace($status)) {
        throw "The source checkout must be clean $When; refusing to label a build with only HEAD."
    }
}

function Get-Sha256([string] $Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

. (Join-Path $PSScriptRoot 'SwarmBuildProvenanceHelpers.ps1')

function Get-GatewayAcceptedArguments([string] $Root, [object] $Target) {
    if ([string]$Target.name -cne 'swarm-gateway') { return @() }
    $source = Get-Content -LiteralPath ([string]$Target.src_path) -Raw
    foreach ($field in @('config', 'data_dir')) {
        $pattern = '(?s)#\[arg\(long\)\]\s*' + [regex]::Escape($field) + '\s*:\s*Option<PathBuf>'
        if ($source -notmatch $pattern) {
            throw "swarm-gateway CLI no longer declares the '$field' long option required by the root launcher."
        }
    }
    return @('--config', '--data-dir')
}

function Copy-ToNewFile([string] $Source, [string] $Destination) {
    $inputStream = [IO.File]::Open($Source, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
    try {
        $outputStream = [IO.File]::Open($Destination, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
        try {
            $inputStream.CopyTo($outputStream)
            $outputStream.Flush($true)
        } finally { $outputStream.Dispose() }
    } finally { $inputStream.Dispose() }
}

if ($Package -cnotin @('swarm-mcp', 'swarm-gateway', 'swarm-cli')) {
    throw 'Package must be exactly one of: swarm-mcp, swarm-gateway, swarm-cli. Build the root swarm-host package with Build-SwarmHostProvenance.ps1.'
}
if (-not (Test-Path -LiteralPath $rootManifest -PathType Leaf) -or
    -not (Test-Path -LiteralPath $lockFile -PathType Leaf) -or
    -not (Test-Path -LiteralPath $toolchainFile -PathType Leaf)) {
    throw 'Expected workspace manifest, lockfile, or pinned toolchain file is missing.'
}

$selectedSpec = $packageMap[$Package]
$targetPath = Get-CanonicalPath $TargetDir 'TargetDir'
$outputPath = Get-CanonicalPath $OutputDir 'OutputDir'
if (-not (Test-Path -LiteralPath $targetPath -PathType Container)) {
    throw 'TargetDir must be an existing shared Cargo target directory; this entrypoint never creates it.'
}
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
    throw 'OutputDir must not already exist; refusing to reuse or overwrite a provenance package.'
}
$targetItem = Get-Item -LiteralPath $targetPath -Force
if (($targetItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
    throw 'TargetDir must not itself be a reparse point.'
}
$outputParent = [IO.Path]::GetDirectoryName($outputPath)
if (-not (Test-Path -LiteralPath $outputParent -PathType Container)) {
    throw 'OutputDir parent must already exist; this entrypoint creates only the final package directory.'
}
Assert-NoReparseTraversal $targetPath
Assert-NoReparseTraversal $outputParent

Push-Location $repoRoot
try {
    $reportedRoot = Invoke-NativeText 'git' @('-C', $repoRoot, 'rev-parse', '--show-toplevel')
    if (-not ([IO.Path]::GetFullPath($reportedRoot)).Equals($repoRoot, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'Selected source directory is not the Git checkout root.'
    }
    $toolchainText = Get-Content -LiteralPath $toolchainFile -Raw
    $channelMatch = [regex]::Match($toolchainText, '(?m)^\s*channel\s*=\s*"([^"]+)"\s*$')
    if (-not $channelMatch.Success) { throw 'Could not resolve a channel from rust-toolchain.toml.' }
    $toolchainChannel = $channelMatch.Groups[1].Value
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

Assert-CleanCheckout $repoRoot 'before metadata resolution'
$commit = Invoke-NativeText 'git' @('-C', $repoRoot, 'rev-parse', '--verify', 'HEAD')
$tree = Invoke-NativeText 'git' @('-C', $repoRoot, 'rev-parse', '--verify', 'HEAD^{tree}')
Push-Location $repoRoot
try {
    $metadataText = Invoke-NativeText 'cargo' @('metadata', '--manifest-path', $rootManifest, '--locked', '--format-version', '1')
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
    if ($Package -ceq 'swarm-mcp' -or $Package -ceq 'swarm-gateway' -or $Package -ceq 'swarm-cli') {
        throw "Package '$Package' is not one actual Cargo workspace member. Integrate the frozen standalone frontend/client package and resolve the root lockfile, then rerun this package selection."
    }
    throw "Package '$Package' must resolve to exactly one actual Cargo workspace member; found $($selectedMatches.Count)."
}
$selected = $selectedMatches[0]
$manifestPath = [IO.Path]::GetFullPath([string]$selected.manifest_path)
$expectedManifest = [IO.Path]::GetFullPath((Join-Path $repoRoot $selectedSpec.manifest))
if (-not $manifestPath.Equals($expectedManifest, [StringComparison]::OrdinalIgnoreCase) -or
    -not (Test-PathWithin $manifestPath $repoRoot)) {
    throw "Package '$Package' resolved to an unexpected manifest path."
}
$targetMatches = @($selected.targets | Where-Object {
    ([string]$_.name -ceq [string]$selectedSpec.binary) -and @($_.kind) -contains 'bin'
})
if ($targetMatches.Count -ne 1) {
    throw "Package '$Package' must expose exactly one expected binary target '$($selectedSpec.binary)'."
}
$dependencyPins = Get-ResolvedDependencyPins $metadata $selected $repoRoot $lockFile
if ($Package -ceq 'swarm-cli') {
    $forbiddenTransitive = @($dependencyPins | Where-Object {
        [string]$_.name -match '\A(?:eliot-swarm-controller|swarm-store|swarm-kernel|swarm-adapter-)'
    })
    if ($forbiddenTransitive.Count -gt 0) {
        throw 'Resolved public CLI dependency graph reaches the root host, Store, kernel, or an adapter package.'
    }
}
$hostIpcProtocolVersion = Get-HostIpcProtocolVersion $repoRoot
$rustcHostTriple = Get-RustcHostTriple $rustcVersion
$gatewayArguments = Get-GatewayAcceptedArguments $repoRoot $targetMatches[0]
$requiredHostPackages = @()
$hostLauncher = $null
$hostRuntime = $null
$hostSupervisor = $null
if ($Package -ceq 'swarm-cli') {
    $requiredHostPackages = Get-PublicCliHostRequirement $targetMatches[0] $selected
    $hostLauncher = [ordered]@{ package_name = 'eliot-swarm-controller'; binary_target = 'swarm-host'; required_arguments = @() }
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
}
if ([string]$selected.version -cnotmatch '\A[0-9A-Za-z.+-]{1,128}\z') {
    throw "Package '$Package' has an invalid Cargo package version."
}
Assert-CleanCheckout $repoRoot 'before build'

$buildArguments = @(
    'build', '--manifest-path', $rootManifest,
    '--package', $Package, '--bin', [string]$selectedSpec.binary,
    '--release', '--locked', '--target-dir', $targetPath
)
Push-Location $repoRoot
try {
    & cargo @buildArguments
    $buildExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
if ($buildExitCode -ne 0) { throw "Selected frontend package build failed with exit code $buildExitCode." }
Assert-CleanCheckout $repoRoot 'after build'

$binaryLeaf = [string]$selectedSpec.binary + '.exe'
$binarySource = Join-Path (Join-Path $targetPath 'release') $binaryLeaf
if (-not (Test-Path -LiteralPath $binarySource -PathType Leaf)) {
    throw "Cargo reported success but expected '$binaryLeaf' is absent from the shared release target."
}
Assert-NoReparseTraversal $binarySource
$sourceBinaryHash = Get-Sha256 $binarySource
$sourceBinaryBytes = (Get-Item -LiteralPath $binarySource -Force).Length
if ($sourceBinaryBytes -le 0 -or $sourceBinaryBytes -gt 1073741824) {
    throw 'Selected frontend executable is empty or exceeds the 1 GiB package limit.'
}
if (Test-Path -LiteralPath $outputPath) {
    throw 'OutputDir appeared during the build; refusing to overwrite it.'
}
$outputParent = [IO.Path]::GetDirectoryName($outputPath)
$stagePath = Join-Path $outputParent ('.' + [IO.Path]::GetFileName($outputPath) + '.' + [guid]::NewGuid().ToString('N') + '.stage')
if (Test-Path -LiteralPath $stagePath) { throw 'Unique frontend staging path unexpectedly exists.' }
[void][IO.Directory]::CreateDirectory($stagePath)
$binaryOutputDir = Join-Path $stagePath 'bin'
$binaryOutput = Join-Path $binaryOutputDir $binaryLeaf
$provenancePath = Join-Path $stagePath 'build-manifest.json'
$packagePublished = $false
try {
    Assert-NoReparseTraversal $stagePath
    [void][IO.Directory]::CreateDirectory($binaryOutputDir)
    Assert-NoReparseTraversal $binaryOutputDir
    Copy-ToNewFile $binarySource $binaryOutput
    Assert-NoReparseTraversal $binaryOutput
    $artifactHash = Get-Sha256 $binaryOutput
    if ($artifactHash -cne $sourceBinaryHash -or (Get-Item -LiteralPath $binaryOutput).Length -ne $sourceBinaryBytes) {
        throw 'Copied frontend executable differs from the shared-target release image.'
    }

$manifestRelative = [IO.Path]::GetRelativePath($repoRoot, $manifestPath).Replace('\', '/')
$sourceFacts = [ordered]@{
    commit = $commit
    tree = $tree
    checkout_clean_before = $true
    checkout_clean_after = $true
    cargo_toml_sha256 = Get-Sha256 $rootManifest
    cargo_lock_sha256 = Get-Sha256 $lockFile
    rust_toolchain_toml_sha256 = Get-Sha256 $toolchainFile
    package_manifest_sha256 = Get-Sha256 $manifestPath
}
$buildFacts = [ordered]@{
    role = [string]$selectedSpec.role
    profile = 'release'
    package_name = [string]$selected.name
    package_version = [string]$selected.version
    package_manifest = $manifestRelative
    binary_target = [string]$selectedSpec.binary
    cargo_arguments = @($buildArguments)
    external_target_dir = $targetPath
}
$artifact = [ordered]@{
    target_name = [string]$selectedSpec.binary
    file = "bin/$binaryLeaf"
    bytes = $sourceBinaryBytes
    source_sha256 = $sourceBinaryHash
    artifact_sha256 = $artifactHash
}
$compatibilityFacts = [ordered]@{
    required_sibling_binaries = @($requiredHostPackages)
    host_ipc = [ordered]@{ protocol_version = $hostIpcProtocolVersion }
    target = [ordered]@{ rustc_host_triple = $rustcHostTriple }
    accepted_launcher_arguments = @($gatewayArguments)
}
if ($null -ne $hostLauncher) { $compatibilityFacts['host_launcher'] = $hostLauncher }
if ($null -ne $hostRuntime) { $compatibilityFacts['host_runtime'] = $hostRuntime }
if ($null -ne $hostSupervisor) { $compatibilityFacts['host_supervisor'] = $hostSupervisor }
$provenance = [ordered]@{
    schema_version = 1
    format = 'eliot.frontend_build_manifest.v1'
    source = $sourceFacts
    toolchain = [ordered]@{
        pinned_channel = $toolchainChannel
        active_toolchain = $activeToolchain
        rustc_version_verbose = $rustcVersion
        cargo_version_verbose = $cargoVersion
    }
    build = $buildFacts
    dependency_pins = @($dependencyPins)
    dependency_graph_scope = [ordered]@{
        source = 'cargo_metadata_workspace_resolve'
        package_edges = 'reachable_from_selected_package_in_workspace_resolved_graph_may_overapprox'
        feature_sets = 'workspace_unified_not_package_specific_build_features'
    }
    artifacts = @($artifact)
    compatibility = $compatibilityFacts
    installation = [ordered]@{
        installed = $false
        registered = $false
        configured = $false
        activated = $false
    }
}
    $json = ConvertTo-Json -InputObject $provenance -Depth 10
    $manifestStream = [IO.File]::Open($provenancePath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
    try {
        $manifestBytes = [Text.UTF8Encoding]::new($false).GetBytes($json + [Environment]::NewLine)
        $manifestStream.Write($manifestBytes, 0, $manifestBytes.Length)
        $manifestStream.Flush($true)
    } finally {
        $manifestStream.Dispose()
    }
    if (Test-Path -LiteralPath $outputPath) {
        throw 'OutputDir appeared during package creation; refusing to overwrite it.'
    }
    [IO.Directory]::Move($stagePath, $outputPath)
    $packagePublished = $true
    $binaryOutputDir = Join-Path $outputPath 'bin'
    $binaryOutput = Join-Path $binaryOutputDir $binaryLeaf
    $provenancePath = Join-Path $outputPath 'build-manifest.json'
} catch {
    $packageFailure = $_
    $cleanupFailures = [Collections.Generic.List[string]]::new()
    foreach ($stagedFile in @($binaryOutput, $provenancePath)) {
        try {
            if (Test-Path -LiteralPath $stagedFile -PathType Leaf) {
                Remove-Item -LiteralPath $stagedFile -Force -Confirm:$false
            }
        } catch {
            $cleanupFailures.Add("could not remove staged file ${stagedFile}: $($_.Exception.Message)")
        }
    }
    if (Test-Path -LiteralPath $binaryOutputDir -PathType Container) {
        try { [IO.Directory]::Delete($binaryOutputDir, $false) }
        catch { $cleanupFailures.Add("could not remove staging directory ${binaryOutputDir}: $($_.Exception.Message)") }
    }
    $rootToClean = if ($packagePublished) { $outputPath } else { $stagePath }
    if (Test-Path -LiteralPath $rootToClean -PathType Container) {
        try { [IO.Directory]::Delete($rootToClean, $false) }
        catch { $cleanupFailures.Add("could not remove package directory ${rootToClean}: $($_.Exception.Message)") }
    }
    if ($cleanupFailures.Count -gt 0) {
        throw "Frontend packaging failed: $($packageFailure.Exception.Message) Staging cleanup notes: $($cleanupFailures -join '; '). Inspect and remove the exact staging path before retrying."
    }
    throw $packageFailure
}

[pscustomobject]@{
    status = 'packaged'
    package = $Package
    role = [string]$selectedSpec.role
    binary = $binaryOutput
    executable_sha256 = $artifactHash
    source_commit = $commit
    source_tree = $tree
    build_manifest = $provenancePath
    shared_target = $targetPath
}
