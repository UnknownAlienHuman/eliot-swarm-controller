[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string] $HostExecutable,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string] $PackageExecutable
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not $IsWindows) {
    throw 'This helper packaging entry accepts the Windows .exe package only.'
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
            throw "Reparse points are not accepted in package or host paths: $cursor"
        }
    }
}

function Assert-AbsoluteExecutable([string] $Path, [string] $Label, [string] $ExpectedLeaf) {
    if (-not [IO.Path]::IsPathFullyQualified($Path)) {
        throw "$Label must be an absolute path."
    }
    $fullPath = [IO.Path]::GetFullPath($Path)
    Assert-NoReparseTraversal $fullPath
    $item = Get-Item -LiteralPath $fullPath -Force
    if ($item.PSIsContainer -or $item.Length -le 0 -or
        [IO.Path]::GetExtension($fullPath) -ine '.exe' -or
        [IO.Path]::GetFileName($fullPath) -cne $ExpectedLeaf) {
        throw "$Label must be a non-empty $ExpectedLeaf regular executable."
    }
    return $fullPath
}

function Get-Sha256([string] $Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

$hostPath = Assert-AbsoluteExecutable $HostExecutable 'HostExecutable' 'swarm.exe'
$packagePath = Assert-AbsoluteExecutable $PackageExecutable 'PackageExecutable' 'swarm-module-owner.exe'
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
    $manifest.build.package_name -cne 'swarm-process' -or
    $manifest.source.checkout_clean_before -ne $true -or
    $manifest.source.checkout_clean_after -ne $true -or
    @($manifest.build.binary_targets | Where-Object { [string]$_ -ceq 'swarm-module-owner' }).Count -ne 1) {
    throw 'Build manifest does not identify a clean swarm-process package containing swarm-module-owner.'
}
$artifactRows = @($manifest.artifacts | Where-Object {
    [string]$_.target_name -ceq 'swarm-module-owner' -and
    [string]$_.file -ceq 'bin/swarm-module-owner.exe'
})
if ($artifactRows.Count -ne 1 -or
    $packagePath -cne [IO.Path]::GetFullPath((Join-Path $packageRoot 'bin/swarm-module-owner.exe'))) {
    throw 'Build manifest does not identify this exact helper executable path.'
}
$packageHash = [string]$artifactRows[0].artifact_sha256
if ($packageHash -cnotmatch '\A[0-9a-f]{64}\z' -or (Get-Sha256 $packagePath) -cne $packageHash) {
    throw 'Package helper bytes do not match the build manifest SHA-256.'
}

$hostDirectory = [IO.Path]::GetDirectoryName($hostPath)
$destination = Join-Path $hostDirectory 'swarm-module-owner.exe'
Assert-NoReparseTraversal $hostDirectory
if (([IO.Path]::GetFullPath($destination)).Equals($hostPath, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'The host executable and owner helper destination cannot be the same file.'
}

$status = 'what_if'
if (Test-Path -LiteralPath $destination) {
    Assert-NoReparseTraversal $destination
    $existing = Get-Item -LiteralPath $destination -Force
    if ($existing.PSIsContainer) { throw 'The owner helper destination is a directory.' }
    if ((Get-Sha256 $destination) -cne $packageHash) {
        throw 'A different owner helper already exists beside the configured host; refusing overwrite.'
    }
    $status = 'unchanged'
} elseif ($PSCmdlet.ShouldProcess($destination, 'Install the prebuilt swarm-module-owner helper beside the host')) {
    $temporary = Join-Path $hostDirectory ('.swarm-module-owner-' + [guid]::NewGuid().ToString('N') + '.stage')
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
            throw 'Staged owner helper bytes do not match the packaged SHA-256.'
        }
        [IO.File]::Move($temporary, $destination)
        Assert-NoReparseTraversal $destination
        if ((Get-Sha256 $destination) -cne $packageHash) {
            throw 'Installed owner helper bytes do not match the packaged SHA-256.'
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
    owner_helper = [IO.Path]::GetFullPath($destination)
    owner_helper_sha256 = $packageHash
}
