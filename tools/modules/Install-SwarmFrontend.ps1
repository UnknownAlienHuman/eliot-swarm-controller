[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
param(
    [Parameter(Mandatory = $true)] [ValidateNotNullOrEmpty()] [string] $PackageDirectory,
    [Parameter(Mandatory = $true)] [ValidateNotNullOrEmpty()] [string] $InstallDirectory
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (-not $IsWindows) { throw 'This create-only installer accepts Windows frontend packages only.' }

$packageMap = @{
    'eliot-swarm-controller' = [ordered]@{
        manifest = 'Cargo.toml'
        binary = 'swarm'
        role = 'host_cli'
        required_siblings = @()
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
        binary = 'swarm-cli'
        role = 'cli_client'
        required_siblings = @()
    }
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
        } elseif ($null -ne $pin.checksum) {
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

function Assert-CompatibilityManifest(
    [object] $Manifest,
    [string] $PackageName,
    [System.Collections.IDictionary] $Spec,
    [string] $Label
) {
    if ($Manifest.compatibility -isnot [System.Collections.IDictionary]) {
        throw "$Label is missing its explicit host IPC/launcher compatibility record. Repackage it with the updated provenance builder."
    }
    [void](Get-HostIpcVersion $Manifest $Label)
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
        $launcher = $Manifest.compatibility.gateway_launcher
        $arguments = @($launcher.required_arguments | ForEach-Object { [string]$_ })
        if ($launcher.package_name -cne 'swarm-gateway' -or
            $launcher.binary_target -cne 'swarm-gateway' -or
            $arguments.Count -ne 2 -or $arguments[0] -cne '--config' -or $arguments[1] -cne '--data-dir') {
            throw "$Label does not declare the observed swarm-gateway binary and argv contract. Rebuild root swarm with the updated provenance builder."
        }
    } elseif ($PackageName -ceq 'swarm-gateway') {
        $arguments = @($Manifest.compatibility.accepted_launcher_arguments | ForEach-Object { [string]$_ })
        foreach ($required in @('--config', '--data-dir')) {
            if (@($arguments | Where-Object { $_ -ceq $required }).Count -ne 1) {
                throw "$Label does not advertise the '$required' option forwarded by the root swarm launcher. Rebuild the gateway package with the updated provenance builder."
            }
        }
    }
}

function Assert-HostIpcCompatible([object] $Left, [string] $LeftLabel, [object] $Right, [string] $RightLabel) {
    $leftVersion = Get-HostIpcVersion $Left $LeftLabel
    $rightVersion = Get-HostIpcVersion $Right $RightLabel
    $leftTarget = [string]$Left.compatibility.target.rustc_host_triple
    $rightTarget = [string]$Right.compatibility.target.rustc_host_triple
    if ($leftVersion -ne $rightVersion -or $leftTarget -cne $rightTarget) {
        throw "Frontend runtime contract mismatch: $LeftLabel uses client.hello v$leftVersion / $leftTarget while $RightLabel uses v$rightVersion / $rightTarget. Install compatible versioned frontend artifacts together."
    }
}

function Assert-GatewayLauncherCompatible([object] $HostManifest, [object] $GatewayManifest) {
    $launcher = $HostManifest.compatibility.gateway_launcher
    if ($launcher.package_name -cne 'swarm-gateway' -or $launcher.binary_target -cne 'swarm-gateway') {
        throw 'The root swarm package does not name the actual swarm-gateway sibling binary.'
    }
    $required = @($launcher.required_arguments | ForEach-Object { [string]$_ })
    $accepted = @($GatewayManifest.compatibility.accepted_launcher_arguments | ForEach-Object { [string]$_ })
    foreach ($argument in $required) {
        if (@($accepted | Where-Object { $_ -ceq $argument }).Count -ne 1) {
            throw "The installed swarm-gateway manifest does not declare root swarm's required '$argument' argument. Install a compatible gateway artifact into a new versioned directory; no files were changed."
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
    if ($RequirePackageManifestHash) {
        Assert-Hash $Source.package_manifest_sha256 "$Prefix.package_manifest_sha256"
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
    $paths = @($binaryPath, $buildPath, $receiptPath)
    $present = @($paths | Where-Object { Test-Path -LiteralPath $_ })
    if ($present.Count -eq 0) { return $null }
    if ($present.Count -ne $paths.Count) {
        $missing = @($paths | Where-Object { -not (Test-Path -LiteralPath $_) })
        if ($Name -ceq 'swarm-gateway' -and $RequestedPackage -ceq 'eliot-swarm-controller') {
            throw "The existing swarm gateway sibling is not provenance-complete; missing $($missing -join ', '). Use a new empty install directory, install the matching swarm-gateway package there first, then install the root swarm CLI."
        }
        throw "Installed frontend '$Name' is incomplete; missing $($missing -join ', '). Use a new empty install directory or restore the exact package files before installing another frontend."
    }
    foreach ($path in $paths) {
        Assert-NoReparseTraversal $path
        $item = Get-Item -LiteralPath $path -Force
        if ($item.PSIsContainer -or $item.Length -le 0) { throw "Installed frontend file is not a non-empty regular file: $path" }
    }

    $receipt = Read-JsonMap $receiptPath 'Installed frontend receipt'
    $manifest = Read-JsonMap $buildPath 'Installed frontend build manifest'
    $expectedManifestFormat = if ($Name -ceq 'eliot-swarm-controller') {
        'eliot.module_build_manifest.v1'
    } else {
        'eliot.frontend_build_manifest.v1'
    }
    $buildTargetValid = if ($Name -ceq 'eliot-swarm-controller') {
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
        ($Name -cne 'eliot-swarm-controller' -and $manifest.build.role -cne [string]$Spec.role) -or
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
    Assert-SourceIdentity $manifest.source 'Installed build source' ($Name -cne 'eliot-swarm-controller')
    Assert-DependencyPins $manifest "Installed frontend '$Name' build manifest"
    Assert-CompatibilityManifest $manifest $Name $Spec "Installed frontend '$Name' build manifest"
    Assert-Hash $receipt.package_manifest_sha256 'receipt.package_manifest_sha256'
    $expectedPackageManifestHash = if ($Name -ceq 'eliot-swarm-controller') {
        [string]$manifest.source.cargo_toml_sha256
    } else {
        [string]$manifest.source.package_manifest_sha256
    }
    if ([string]$receipt.package_manifest_sha256 -cne $expectedPackageManifestHash) {
        throw "Installed frontend '$Name' package-manifest pin does not match its source manifest."
    }
    Assert-Hash $receipt.executable_sha256 'receipt.executable_sha256'
    Assert-Hash $receipt.build_manifest_sha256 'receipt.build_manifest_sha256'
    if ((Get-Sha256 $buildPath) -cne [string]$receipt.build_manifest_sha256 -or
        (Get-Sha256 $binaryPath) -cne [string]$receipt.executable_sha256 -or
        [string]$manifest.source.commit -cne [string]$receipt.source_commit -or
        [string]$manifest.source.tree -cne [string]$receipt.source_tree -or
        [string]$manifest.source.cargo_lock_sha256 -cne [string]$receipt.cargo_lock_sha256 -or
        [string]$manifest.source.cargo_toml_sha256 -cne [string]$receipt.cargo_toml_sha256 -or
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
    throw "Build manifest names unsupported package '$packageName'. Select swarm-mcp, swarm-gateway, swarm-cli, or eliot-swarm-controller."
}
$selectedSpec = $packageMap[$packageName]
$isHostPackage = $packageName -ceq 'eliot-swarm-controller'
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
Assert-SourceIdentity $provenance.source 'Build source' (-not $isHostPackage)
Assert-DependencyPins $provenance 'Build manifest'
Assert-CompatibilityManifest $provenance $packageName $selectedSpec 'Build manifest'
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
foreach ($requiredName in $expectedSiblings) {
    $requiredBinary = [string]$packageMap[$requiredName].binary + '.exe'
    if ($requiredName -cne 'swarm-gateway') {
        throw "Unrecognized required frontend sibling '$requiredName'; update the installer map before installing this package."
    }
    if (-not (Test-Path -LiteralPath (Join-Path $installRoot $requiredBinary) -PathType Leaf)) {
        throw "Required sibling '$requiredBinary' is missing. Install the matching '$requiredName' package to '$installRoot' first."
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
        $createdDestinations = [Collections.Generic.List[string]]::new()
        $receiptHash = $null
        try {
            Copy-ToStage $packageBinary $stageBinary
            Copy-ToStage $manifestPath $stageBuild
            $packageManifestHash = if ($isHostPackage) {
                [string]$provenance.source.cargo_toml_sha256
            } else {
                [string]$provenance.source.package_manifest_sha256
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
                source_commit = [string]$provenance.source.commit
                source_tree = [string]$provenance.source.tree
                cargo_toml_sha256 = [string]$provenance.source.cargo_toml_sha256
                cargo_lock_sha256 = [string]$provenance.source.cargo_lock_sha256
                rust_toolchain_toml_sha256 = [string]$provenance.source.rust_toolchain_toml_sha256
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
            foreach ($destination in @($binaryDestination, $buildDestination, $receiptDestination)) {
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
            foreach ($destination in @($binaryDestination, $buildDestination, $receiptDestination)) {
                Assert-NoReparseTraversal $destination
            }
            if ((Get-Sha256 $binaryDestination) -cne $executableHash -or
                (Get-Sha256 $buildDestination) -cne $buildManifestHash) {
                throw 'Installed frontend bytes differ from the staged package pins.'
            }
            $status = 'installed'
        } catch {
            $installFailure = $_
            $cleanupFailures = [Collections.Generic.List[string]]::new()
            $cleanupHashes = @{}
            $cleanupHashes[$binaryDestination] = $executableHash
            $cleanupHashes[$buildDestination] = $buildManifestHash
            $cleanupHashes[$receiptDestination] = $receiptHash
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
            if ($cleanupFailures.Count -gt 0) {
                throw "Create-only install failed: $($installFailure.Exception.Message) Rollback notes: $($cleanupFailures -join '; '). Inspect those exact paths before retrying."
            }
            throw $installFailure
        } finally {
            foreach ($stage in @($stageBinary, $stageBuild, $stageReceipt)) {
                if (Test-Path -LiteralPath $stage -PathType Leaf) { Remove-Item -LiteralPath $stage -Force -Confirm:$false }
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
    source_commit = [string]$provenance.source.commit
    source_tree = [string]$provenance.source.tree
}
