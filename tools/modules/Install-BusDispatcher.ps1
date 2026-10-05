[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string] $PackageExecutable,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string] $InstallDirectory
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not $IsWindows) {
    throw 'This copy-only installer accepts the Windows .exe artifact only.'
}

function Assert-NoReparseTraversal([string] $Path) {
    $fullPath = [IO.Path]::GetFullPath($Path)
    $root = [IO.Path]::GetPathRoot($fullPath)
    if ([string]::IsNullOrWhiteSpace($root)) {
        throw 'Path has no filesystem root.'
    }
    $cursor = $root
    $segments = $fullPath.Substring($root.Length).Split(
        [char[]]@([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar),
        [StringSplitOptions]::RemoveEmptyEntries
    )
    foreach ($segment in $segments) {
        $cursor = [IO.Path]::Combine($cursor, $segment)
        $item = Get-Item -LiteralPath $cursor -Force -ErrorAction SilentlyContinue
        if ($null -ne $item -and
            ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Reparse points are not accepted in package or install paths: $cursor"
        }
    }
}

function Assert-AbsoluteDispatcher([string] $Path) {
    if (-not [IO.Path]::IsPathFullyQualified($Path)) {
        throw 'PackageExecutable must be an absolute path.'
    }
    $fullPath = [IO.Path]::GetFullPath($Path)
    Assert-NoReparseTraversal $fullPath
    $item = Get-Item -LiteralPath $fullPath -Force
    if ($item.PSIsContainer -or $item.Length -le 0 -or
        [IO.Path]::GetExtension($fullPath) -ine '.exe' -or
        [IO.Path]::GetFileName($fullPath) -cne 'swarm-bus-dispatcher.exe') {
        throw 'PackageExecutable must be the non-empty swarm-bus-dispatcher.exe regular file.'
    }
    return $fullPath
}

function Get-Sha256([string] $Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

$packagePath = Assert-AbsoluteDispatcher $PackageExecutable
if (-not [IO.Path]::IsPathFullyQualified($InstallDirectory)) {
    throw 'InstallDirectory must be an absolute path.'
}
$installRoot = [IO.Path]::GetFullPath($InstallDirectory)
Assert-NoReparseTraversal $installRoot
$installItem = Get-Item -LiteralPath $installRoot -Force
if (-not $installItem.PSIsContainer) {
    throw 'InstallDirectory must already exist as a directory.'
}

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
    $manifest.build.package_name -cne 'swarm-bus' -or
    $manifest.build.package_manifest -cne 'crates/swarm-bus/Cargo.toml' -or
    [string]$manifest.build.package_version -cnotmatch '\A[0-9A-Za-z.+-]{1,128}\z' -or
    $manifest.build.profile -cne 'release' -or
    $manifest.source.checkout_clean_before -ne $true -or
    $manifest.source.checkout_clean_after -ne $true -or
    [string]$manifest.source.commit -cnotmatch '\A[0-9a-f]{40,64}\z' -or
    [string]$manifest.source.tree -cnotmatch '\A[0-9a-f]{40,64}\z' -or
    [string]$manifest.source.cargo_toml_sha256 -cnotmatch '\A[0-9a-f]{64}\z' -or
    [string]$manifest.source.cargo_lock_sha256 -cnotmatch '\A[0-9a-f]{64}\z' -or
    [string]$manifest.source.rust_toolchain_toml_sha256 -cnotmatch '\A[0-9a-f]{64}\z' -or
    @($manifest.build.binary_targets).Count -ne 1 -or
    [string]$manifest.build.binary_targets[0] -cne 'swarm-bus-dispatcher' -or
    $manifest.dependency_pins -isnot [array] -or
    @($manifest.dependency_pins).Count -eq 0 -or
    $manifest.dependency_graph_scope.source -cne 'cargo_metadata_workspace_resolve') {
    throw 'Build manifest does not identify a clean release swarm-bus package and positive dispatcher coordinate.'
}

$artifactRows = @($manifest.artifacts | Where-Object {
    [string]$_.target_name -ceq 'swarm-bus-dispatcher' -and
    [string]$_.file -ceq 'bin/swarm-bus-dispatcher.exe'
})
$expectedPackagePath = [IO.Path]::GetFullPath((Join-Path $packageRoot 'bin/swarm-bus-dispatcher.exe'))
if ($artifactRows.Count -ne 1 -or $packagePath -cne $expectedPackagePath) {
    throw 'Build manifest does not identify this exact bus dispatcher executable path.'
}

$packageHash = [string]$artifactRows[0].artifact_sha256
$sourceHash = [string]$artifactRows[0].source_sha256
$packageBytes = (Get-Item -LiteralPath $packagePath -Force).Length
if ($packageBytes -gt 536870912 -or
    $packageHash -cnotmatch '\A[0-9a-f]{64}\z' -or
    $sourceHash -cnotmatch '\A[0-9a-f]{64}\z' -or
    $sourceHash -cne $packageHash -or
    $artifactRows[0].bytes -ne $packageBytes -or
    (Get-Sha256 $packagePath) -cne $packageHash) {
    throw 'Package bus dispatcher bytes, size, or SHA-256 do not match the build manifest.'
}

$destination = Join-Path $installRoot 'swarm-bus-dispatcher.exe'
if ([IO.Path]::GetFullPath($packagePath).Equals(
        [IO.Path]::GetFullPath($destination), [StringComparison]::OrdinalIgnoreCase)) {
    throw 'PackageExecutable and the installed dispatcher destination must be different paths.'
}

$status = 'what_if'
if (Test-Path -LiteralPath $destination) {
    Assert-NoReparseTraversal $destination
    $existing = Get-Item -LiteralPath $destination -Force
    if ($existing.PSIsContainer) {
        throw 'Bus dispatcher destination is a directory.'
    }
    if ((Get-Sha256 $destination) -cne $packageHash) {
        throw 'A different bus dispatcher already exists in the selected directory; refusing overwrite.'
    }
    $status = 'unchanged'
} elseif ($PSCmdlet.ShouldProcess($destination, 'Install the prebuilt standalone bus dispatcher')) {
    $temporary = Join-Path $installRoot ('.swarm-bus-dispatcher-' + [guid]::NewGuid().ToString('N') + '.stage')
    try {
        $inputStream = [IO.File]::Open(
            $packagePath, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read
        )
        try {
            $outputStream = [IO.File]::Open(
                $temporary, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None
            )
            try {
                $inputStream.CopyTo($outputStream)
                $outputStream.Flush($true)
            } finally { $outputStream.Dispose() }
        } finally { $inputStream.Dispose() }
        Assert-NoReparseTraversal $temporary
        if ((Get-Sha256 $temporary) -cne $packageHash) {
            throw 'Staged bus dispatcher differs from the build-manifest digest.'
        }
        [IO.File]::Move($temporary, $destination)
        Assert-NoReparseTraversal $destination
        if ((Get-Sha256 $destination) -cne $packageHash) {
            throw 'Installed bus dispatcher differs from the build-manifest digest.'
        }
        $status = 'installed'
    } finally {
        if (Test-Path -LiteralPath $temporary -PathType Leaf) {
            Remove-Item -LiteralPath $temporary
        }
    }
}

[pscustomobject]@{
    status = $status
    coordinate = "swarm-bus@$($manifest.build.package_version)/swarm-bus-dispatcher"
    executable = [IO.Path]::GetFullPath($destination)
    sha256 = $packageHash
    image_sha256 = $packageHash
    source_sha256 = $sourceHash
    artifact_sha256 = $packageHash
    byte_length = $packageBytes
    package_name = 'swarm-bus'
    target_name = 'swarm-bus-dispatcher'
    build_manifest = [IO.Path]::GetFullPath($manifestPath)
    build_profile = [string]$manifest.build.profile
    artifact_id = 'swarm-bus-dispatcher.1'
    version = [string]$manifest.build.package_version
    source_commit = [string]$manifest.source.commit
    source_tree = [string]$manifest.source.tree
}
