[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('swarm-automation-worker', 'swarm-forge-worker')]
    [string] $WorkerCoordinate,

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
    throw 'This copy-only installer accepts Windows .exe packages only.'
}

$contracts = @{
    'swarm-automation-worker' = [ordered]@{
        package_name = 'swarm-automation'
        package_manifest = 'crates/swarm-automation/Cargo.toml'
        binary_target = 'swarm-automation-worker'
        executable = 'swarm-automation-worker.exe'
        artifact_id = 'swarm-automation-worker.1'
    }
    'swarm-forge-worker' = [ordered]@{
        package_name = 'swarm-forge-worker'
        package_manifest = 'crates/swarm-forge-worker/Cargo.toml'
        binary_target = 'swarm-forge-worker'
        executable = 'swarm-forge-worker.exe'
        artifact_id = 'swarm-forge-worker.1'
    }
}
$contract = $contracts[$WorkerCoordinate]

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
        (-not [string]::IsNullOrEmpty($ExpectedLeaf) -and
            [IO.Path]::GetFileName($fullPath) -cne $ExpectedLeaf)) {
        $suffix = if ([string]::IsNullOrEmpty($ExpectedLeaf)) { '.exe' } else { $ExpectedLeaf }
        throw "$Label must be a non-empty $suffix regular executable."
    }
    return $fullPath
}

function Get-Sha256([string] $Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

$hostPath = Assert-AbsoluteExecutable $HostExecutable 'HostExecutable' $null
$packagePath = Assert-AbsoluteExecutable $PackageExecutable 'PackageExecutable' $contract.executable
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
    $manifest.build.package_name -cne $contract.package_name -or
    $manifest.build.package_manifest -cne $contract.package_manifest -or
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
    [string]$manifest.build.binary_targets[0] -cne $contract.binary_target -or
    $manifest.dependency_pins -isnot [array] -or
    @($manifest.dependency_pins).Count -eq 0 -or
    $manifest.dependency_graph_scope.source -cne 'cargo_metadata_workspace_resolve') {
    throw "Build manifest does not identify a clean release $($contract.package_name) package and positive binary coordinate."
}

$artifactRows = @($manifest.artifacts | Where-Object {
    [string]$_.target_name -ceq $contract.binary_target -and
    [string]$_.file -ceq "bin/$($contract.executable)"
})
$expectedPackagePath = [IO.Path]::GetFullPath((Join-Path $packageRoot "bin/$($contract.executable)"))
if ($artifactRows.Count -ne 1 -or $packagePath -cne $expectedPackagePath) {
    throw 'Build manifest does not identify this exact standalone worker executable path.'
}

$packageHash = [string]$artifactRows[0].artifact_sha256
$sourceHash = [string]$artifactRows[0].source_sha256
$packageBytes = (Get-Item -LiteralPath $packagePath -Force).Length
if ($packageHash -cnotmatch '\A[0-9a-f]{64}\z' -or
    $sourceHash -cnotmatch '\A[0-9a-f]{64}\z' -or
    $sourceHash -cne $packageHash -or
    $artifactRows[0].bytes -ne $packageBytes -or
    (Get-Sha256 $packagePath) -cne $packageHash) {
    throw 'Standalone worker bytes, source provenance, length, or SHA-256 do not match the build manifest.'
}

$hostImageHash = Get-Sha256 $hostPath
if ($hostImageHash -ceq $sourceHash -or $hostImageHash -ceq $packageHash) {
    throw 'The standalone worker and host resolve to the same image/source SHA-256; use independent artifacts.'
}

$hostDirectory = [IO.Path]::GetDirectoryName($hostPath)
$destination = Join-Path $hostDirectory $contract.executable
Assert-NoReparseTraversal $hostDirectory
if ([IO.Path]::GetFullPath($destination).Equals($hostPath, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'The host executable and standalone worker destination cannot be the same file.'
}

$status = 'what_if'
if (Test-Path -LiteralPath $destination) {
    Assert-NoReparseTraversal $destination
    $existing = Get-Item -LiteralPath $destination -Force
    if ($existing.PSIsContainer) {
        throw 'The standalone worker destination is a directory.'
    }
    if ((Get-Sha256 $destination) -cne $packageHash) {
        throw 'A different standalone worker already exists beside the selected host; refusing overwrite.'
    }
    $status = 'unchanged'
} elseif ($PSCmdlet.ShouldProcess($destination, "Install the prebuilt $WorkerCoordinate beside the selected host")) {
    $temporary = Join-Path $hostDirectory ('.eliot-standalone-worker-' + [guid]::NewGuid().ToString('N') + '.stage')
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
            throw 'Staged standalone worker bytes do not match the packaged SHA-256.'
        }
        [IO.File]::Move($temporary, $destination)
        Assert-NoReparseTraversal $destination
        if ((Get-Sha256 $destination) -cne $packageHash) {
            throw 'Installed standalone worker bytes do not match the packaged SHA-256.'
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
    coordinate = "$($contract.package_name)@$($manifest.build.package_version)/$($contract.binary_target)"
    package_name = $contract.package_name
    target_name = $contract.binary_target
    artifact_id = $contract.artifact_id
    version = [string]$manifest.build.package_version
    host_executable = $hostPath
    executable = [IO.Path]::GetFullPath($destination)
    image_sha256 = $packageHash
    source_sha256 = $sourceHash
    artifact_sha256 = $packageHash
    host_image_sha256 = $hostImageHash
    byte_length = $packageBytes
    build_manifest = [IO.Path]::GetFullPath($manifestPath)
    build_profile = [string]$manifest.build.profile
    source_commit = [string]$manifest.source.commit
    source_tree = [string]$manifest.source.tree
}
