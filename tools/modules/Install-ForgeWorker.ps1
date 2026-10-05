[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
param(
    [Parameter(Mandatory = $true)] [ValidateNotNullOrEmpty()] [string] $HostExecutable,
    [Parameter(Mandatory = $true)] [ValidateNotNullOrEmpty()] [string] $PackageExecutable
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (-not $IsWindows) { throw 'This copy-only installer accepts the Windows .exe artifact only.' }

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
            throw "Reparse points are not accepted in package or host paths: $cursor"
        }
    }
}
function Assert-AbsoluteExecutable([string] $Path, [string] $Label, [string] $ExpectedLeaf) {
    if (-not [IO.Path]::IsPathFullyQualified($Path)) { throw "$Label must be an absolute path." }
    $fullPath = [IO.Path]::GetFullPath($Path)
    Assert-NoReparseTraversal $fullPath
    $item = Get-Item -LiteralPath $fullPath -Force
    if ($item.PSIsContainer -or $item.Length -le 0 -or
        [IO.Path]::GetExtension($fullPath) -ine '.exe' -or
        (-not [string]::IsNullOrEmpty($ExpectedLeaf) -and [IO.Path]::GetFileName($fullPath) -cne $ExpectedLeaf)) {
        throw "$Label must be a non-empty .exe regular executable."
    }
    return $fullPath
}
function Get-Sha256([string] $Path) { return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() }

$hostPath = Assert-AbsoluteExecutable $HostExecutable 'HostExecutable' $null
$packagePath = Assert-AbsoluteExecutable $PackageExecutable 'PackageExecutable' 'swarm-forge-worker.exe'
$packageBin = [IO.Path]::GetDirectoryName($packagePath)
$packageRoot = [IO.Path]::GetDirectoryName($packageBin)
$manifestPath = Join-Path $packageRoot 'build-manifest.json'
if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
    throw 'PackageExecutable must come from a build-module-package.ps1 output with build-manifest.json.'
}
Assert-NoReparseTraversal $manifestPath
$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json -AsHashtable
if ($manifest.schema_version -ne 1 -or
    $manifest.format -cne 'eliot.module_build_manifest.v1' -or
    $manifest.build.package_name -cne 'swarm-forge-worker' -or
    $manifest.build.package_manifest -cne 'crates/swarm-forge-worker/Cargo.toml' -or
    [string]$manifest.build.package_version -cnotmatch '\A[0-9A-Za-z.+-]{1,128}\z' -or
    [string]$manifest.source.cargo_toml_sha256 -cnotmatch '\A[0-9a-f]{64}\z' -or
    [string]$manifest.source.cargo_lock_sha256 -cnotmatch '\A[0-9a-f]{64}\z' -or
    [string]$manifest.source.rust_toolchain_toml_sha256 -cnotmatch '\A[0-9a-f]{64}\z' -or
    $manifest.build.profile -cne 'release' -or
    $manifest.source.checkout_clean_before -ne $true -or
    $manifest.source.checkout_clean_after -ne $true -or
    [string]$manifest.source.commit -cnotmatch '\A[0-9a-f]{40,64}\z' -or
    [string]$manifest.source.tree -cnotmatch '\A[0-9a-f]{40,64}\z' -or
    @($manifest.build.binary_targets).Count -ne 1 -or
    [string]$manifest.build.binary_targets[0] -cne 'swarm-forge-worker') {
    throw 'Build manifest does not identify a clean swarm-forge-worker package and source revision.'
}
$artifactRows = @($manifest.artifacts | Where-Object {
    [string]$_.target_name -ceq 'swarm-forge-worker' -and
    [string]$_.file -ceq 'bin/swarm-forge-worker.exe'
})
$expectedPackagePath = [IO.Path]::GetFullPath((Join-Path $packageRoot 'bin/swarm-forge-worker.exe'))
if ($artifactRows.Count -ne 1 -or $packagePath -cne $expectedPackagePath) {
    throw 'Build manifest does not identify this exact Forge worker executable path.'
}
$packageHash = [string]$artifactRows[0].artifact_sha256
$sourceHash = [string]$artifactRows[0].source_sha256
$packageBytes = (Get-Item -LiteralPath $packagePath -Force).Length
if ($packageHash -cnotmatch '\A[0-9a-f]{64}\z' -or
    $sourceHash -cne $packageHash -or
    $artifactRows[0].bytes -ne $packageBytes -or
    (Get-Sha256 $packagePath) -cne $packageHash) {
    throw 'Package Forge worker bytes do not match the build manifest SHA-256.'
}

$hostDirectory = [IO.Path]::GetDirectoryName($hostPath)
$destination = Join-Path $hostDirectory 'swarm-forge-worker.exe'
Assert-NoReparseTraversal $hostDirectory
if ([IO.Path]::GetFullPath($destination).Equals($hostPath, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'The host executable and Forge worker destination cannot be the same file.'
}
$status = 'what_if'
if (Test-Path -LiteralPath $destination) {
    Assert-NoReparseTraversal $destination
    $existing = Get-Item -LiteralPath $destination -Force
    if ($existing.PSIsContainer) { throw 'The Forge worker destination is a directory.' }
    if ((Get-Sha256 $destination) -cne $packageHash) {
        throw 'A different Forge worker already exists beside the selected host; refusing overwrite.'
    }
    $status = 'unchanged'
} elseif ($PSCmdlet.ShouldProcess($destination, 'Install the prebuilt Forge worker beside the selected host')) {
    $temporary = Join-Path $hostDirectory ('.swarm-forge-worker-' + [guid]::NewGuid().ToString('N') + '.stage')
    try {
        $inputStream = [IO.File]::Open($packagePath, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
        try {
            $outputStream = [IO.File]::Open($temporary, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
            try {
                $inputStream.CopyTo($outputStream)
                $outputStream.Flush($true)
            } finally { $outputStream.Dispose() }
        } finally { $inputStream.Dispose() }
        Assert-NoReparseTraversal $temporary
        if ((Get-Sha256 $temporary) -cne $packageHash) { throw 'Staged Forge worker bytes do not match the packaged SHA-256.' }
        [IO.File]::Move($temporary, $destination)
        Assert-NoReparseTraversal $destination
        if ((Get-Sha256 $destination) -cne $packageHash) { throw 'Installed Forge worker bytes do not match the packaged SHA-256.' }
        $status = 'installed'
    } finally {
        if (Test-Path -LiteralPath $temporary -PathType Leaf) { Remove-Item -LiteralPath $temporary }
    }
}
[pscustomobject]@{
    status = $status
    host_executable = $hostPath
    forge_worker = [IO.Path]::GetFullPath($destination)
    forge_worker_sha256 = $packageHash
    source_commit = [string]$manifest.source.commit
    source_tree = [string]$manifest.source.tree
}
