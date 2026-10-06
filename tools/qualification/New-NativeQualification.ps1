#requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $HostExecutable,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedHostSha256,
    [Parameter(Mandatory)][string] $HostBuildManifestPath,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedHostBuildManifestSha256,
    [Parameter(Mandatory)][string] $PublicCliExecutable,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedPublicCliSha256,
    [Parameter(Mandatory)][string] $PublicCliBuildManifestPath,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedPublicCliBuildManifestSha256,
    [Parameter(Mandatory)][string] $HostConfigPath,
    [Parameter(Mandatory)][string] $OutputRoot,
    [Parameter(Mandatory)][ValidatePattern('^[A-Za-z0-9._:-]{1,128}$')][string] $ProjectId,
    [Parameter(Mandatory)][string] $TaskSpecPath,
    [Parameter(Mandatory)][string] $LaunchSettingsPath,
    [Parameter(Mandatory)][string] $OneTurnTextPath,
    [Parameter(Mandatory)][string] $ModuleInstallRoot,
    [Parameter(Mandatory)][string] $ModuleExecutablePath,
    [Parameter(Mandatory)][string] $ModuleDescriptorPath,
    [Parameter(Mandatory)][string] $ModuleBuildManifestPath,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedModuleBuildManifestSha256,
    [Parameter(Mandatory)][string] $ModuleOwnerHelperPath,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedModuleOwnerHelperSha256,
    [ValidateSet('OpenCode', 'Command', 'Codex', 'Antigravity', 'Claude')][string] $Adapter = 'OpenCode',
    [ValidatePattern('^[A-Za-z0-9._-]+/[A-Za-z0-9._:-]+$')]
    [string] $OpenCodeCommandTestModelRef = 'inclusionai/ling-3.1-flash',
    [ValidateRange(30, 300)][int] $TimeoutSeconds = 180,
    [ValidateRange(1, 2147483647)][int] $CodexAppServerPid,
    [string] $CodexAppServerImagePath,
    [switch] $EnableClaude
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:ContractBaseCommit = '410c327dd6172856bd8808da5938b2be9e1e4f9a'
$script:MaxCliOutputCharacters = 4MB
$script:RequestDirectory = $null
$script:RunDirectory = $null
$script:StateDirectory = $null
$script:HostPath = $null
$script:PublicCliPath = $null
$script:ConfigPath = $null
$script:ManagerCredentialPath = $null
$script:OperatorCredentialPath = $null
$script:HostProcess = $null
$script:HostInputOpen = $false
$script:HostPendingProcessId = $null
$script:DispatchSubmitted = $false
$script:CurrentStage = 'preflight'
$script:FailureCode = $null
$script:PublicCliProcessIdsPending = [System.Collections.Generic.List[int]]::new()
$script:TimedOutRequestFiles = [System.Collections.Generic.List[object]]::new()
$script:Stages = [System.Collections.Generic.List[object]]::new()
$script:Report = [ordered]@{
    schema_version = 1
    harness = 'eliot.native_qualification.v1'
    source_contract_base_commit = $script:ContractBaseCommit
    adapter = $Adapter
    run_id = $null
    status = 'running'
    qualification = 'not_qualified'
    started_at_utc = [DateTime]::UtcNow.ToString('o')
    stages = @()
    safe_facts = [ordered]@{}
    limits = [ordered]@{
        task_dispatch_calls_max = 1
        task_dispatch_calls = 0
        mutation_replay = $false
        raw_request_or_result_saved = 'no raw content is included in the receipt; request files are private transient inputs and remain only if their CLI process has not exited'
        provider_account_identity = 'not_proven_by_harness'
        model_consumption = 'not_proven_by_harness'
        task_completion = 'not_claimed'
        binary_source_revision = 'not_attested_by_executable_hash_alone'
        native_process_management = 'attach_only_for_codex; no native process start/stop/reconfigure/kill'
        unsupported = @()
    }
}

function Add-Stage {
    param(
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)][ValidateSet('passed', 'blocked', 'unknown', 'pending', 'observed')][string] $Status,
        [string] $Code,
        [System.Collections.IDictionary] $Facts = @{}
    )

    $safeCode = $null
    if (-not [string]::IsNullOrWhiteSpace($Code) -and $Code -match '\A[A-Z0-9_]{1,64}\z') {
        $safeCode = $Code
    }
    $stage = [ordered]@{ name = $Name; status = $Status }
    if ($safeCode) { $stage.code = $safeCode }
    if ($Facts.Count -gt 0) { $stage.facts = $Facts }
    $script:Stages.Add([pscustomobject]$stage)
    $script:Report.stages = @($script:Stages.ToArray())
    Save-Report
}

function Stop-Qualification {
    param([Parameter(Mandatory)][string] $Code)
    if ($Code -notmatch '\A[A-Z0-9_]{1,64}\z') { $Code = 'QUALIFICATION_BLOCKED' }
    $script:FailureCode = $Code
    throw [System.InvalidOperationException]::new('QUALIFICATION_STOP')
}

function Assert-AbsolutePath {
    param([Parameter(Mandatory)][string] $Path)
    if (-not [System.IO.Path]::IsPathFullyQualified($Path)) { Stop-Qualification 'PATH_MUST_BE_ABSOLUTE' }
    return [System.IO.Path]::GetFullPath($Path)
}

function Assert-NoReparseTraversal {
    param([Parameter(Mandatory)][string] $Path)
    $full = Assert-AbsolutePath $Path
    $root = [System.IO.Path]::GetPathRoot($full)
    if ([string]::IsNullOrWhiteSpace($root)) { Stop-Qualification 'PATH_ROOT_INVALID' }
    $current = $root
    $relative = $full.Substring($root.Length)
    foreach ($component in ($relative -split '[\\/]')) {
        if ([string]::IsNullOrEmpty($component)) { continue }
        $current = Join-Path $current $component
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force
            if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                Stop-Qualification 'REPARSE_PATH_NOT_ALLOWED'
            }
        }
    }
    return $full
}

function Assert-ExistingFile {
    param([Parameter(Mandatory)][string] $Path)
    $full = Assert-NoReparseTraversal $Path
    $item = Get-Item -LiteralPath $full -Force -ErrorAction SilentlyContinue
    if ($null -eq $item -or $item.PSIsContainer) { Stop-Qualification 'INPUT_FILE_NOT_FOUND' }
    return $full
}

function Assert-ExistingDirectory {
    param([Parameter(Mandatory)][string] $Path)
    $full = Assert-NoReparseTraversal $Path
    $item = Get-Item -LiteralPath $full -Force -ErrorAction SilentlyContinue
    if ($null -eq $item -or -not $item.PSIsContainer) { Stop-Qualification 'INPUT_DIRECTORY_NOT_FOUND' }
    return $full
}

function Get-FileSha256 {
    param([Parameter(Mandatory)][string] $Path)
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Get-BuildProvenance {
    param(
        [Parameter(Mandatory)][string] $ManifestPath,
        [Parameter(Mandatory)][string] $ExpectedManifestSha256,
        [Parameter(Mandatory)][string] $BinaryPath,
        [Parameter(Mandatory)][string] $ExpectedPackage,
        [Parameter(Mandatory)][string] $ExpectedTarget,
        [switch] $RequireBinaryPathMatch
    )
    $manifestFile = Assert-ExistingFile $ManifestPath
    $binary = Assert-ExistingFile $BinaryPath
    if ([System.IO.Path]::GetFileName($manifestFile) -cne 'build-manifest.json') { Stop-Qualification 'BUILD_MANIFEST_FILENAME_INVALID' }
    $manifestHash = Get-FileSha256 $manifestFile
    $binaryHash = Get-FileSha256 $binary
    if ($manifestHash -ne $ExpectedManifestSha256.ToLowerInvariant()) { Stop-Qualification 'BUILD_MANIFEST_HASH_MISMATCH' }
    $manifest = Read-JsonFile $manifestFile
    if ($manifest.schema_version -ne 1 -or $manifest.format -ne 'eliot.module_build_manifest.v1' -or
        $manifest.source.checkout_clean_before -ne $true -or $manifest.source.checkout_clean_after -ne $true -or
        $manifest.source.commit -notmatch '\A[a-f0-9]{40}\z' -or $manifest.source.tree -notmatch '\A[a-f0-9]{40}\z' -or
        $manifest.source.cargo_toml_sha256 -notmatch '\A[a-f0-9]{64}\z' -or
        $manifest.source.cargo_lock_sha256 -notmatch '\A[a-f0-9]{64}\z' -or
        $manifest.source.rust_toolchain_toml_sha256 -notmatch '\A[a-f0-9]{64}\z' -or
        $manifest.build.profile -ne 'release' -or $manifest.build.package_name -cne $ExpectedPackage -or
        $manifest.build.binary_targets.Count -ne 1 -or $manifest.build.binary_targets[0] -cne $ExpectedTarget -or
        [string]::IsNullOrWhiteSpace([string]$manifest.build.target_dir) -or
        -not [System.IO.Path]::IsPathFullyQualified([string]$manifest.build.target_dir) -or
        [string]::IsNullOrWhiteSpace([string]$manifest.toolchain.pinned_channel) -or
        [string]::IsNullOrWhiteSpace([string]$manifest.toolchain.active_toolchain) -or
        [string]::IsNullOrWhiteSpace([string]$manifest.toolchain.rustc_version_verbose) -or
        [string]::IsNullOrWhiteSpace([string]$manifest.toolchain.cargo_version_verbose)) {
        Stop-Qualification 'BUILD_MANIFEST_CONTRACT_INVALID'
    }
    $arguments = @($manifest.build.cargo_arguments | ForEach-Object { [string]$_ })
    foreach ($requiredArgument in @('--locked')) {
        if ($requiredArgument -notin $arguments) { Stop-Qualification 'BUILD_MANIFEST_ARGUMENTS_MISMATCH' }
    }
    $argumentPairs = [ordered]@{
        '--package' = $ExpectedPackage
        '--bin' = $ExpectedTarget
        '--profile' = 'release'
        '--target-dir' = [string]$manifest.build.target_dir
    }
    foreach ($option in $argumentPairs.Keys) {
        $optionIndex = [Array]::IndexOf($arguments, [string]$option)
        if ($optionIndex -lt 0 -or $optionIndex + 1 -ge $arguments.Count -or $arguments[$optionIndex + 1] -cne [string]$argumentPairs[$option]) {
            Stop-Qualification 'BUILD_MANIFEST_ARGUMENTS_MISMATCH'
        }
    }
    $artifacts = @($manifest.artifacts)
    if ($artifacts.Count -ne 1 -or $artifacts[0].target_name -cne $ExpectedTarget -or
        $artifacts[0].file -cne ('bin/' + $ExpectedTarget + '.exe') -or
        [long]$artifacts[0].bytes -ne (Get-Item -LiteralPath $binary).Length -or
        $artifacts[0].source_sha256 -cne $binaryHash -or $artifacts[0].artifact_sha256 -cne $binaryHash) {
        Stop-Qualification 'BUILD_ARTIFACT_IDENTITY_MISMATCH'
    }
    $builtBinary = Assert-ExistingFile (Join-Path (Split-Path -LiteralPath $manifestFile -Parent) ('bin\' + $ExpectedTarget + '.exe'))
    if ((Get-FileSha256 $builtBinary) -cne $binaryHash -or
        (Get-Item -LiteralPath $builtBinary).Length -ne (Get-Item -LiteralPath $binary).Length) {
        Stop-Qualification 'BUILD_OUTPUT_BINARY_MISMATCH'
    }
    if ($RequireBinaryPathMatch -and -not [string]::Equals($builtBinary, $binary, [StringComparison]::OrdinalIgnoreCase)) {
        Stop-Qualification 'SWARM_IMAGE_IS_NOT_BUILD_OUTPUT'
    }
    $installation = $manifest.installation
    foreach ($name in @('descriptor_generated', 'installed', 'registered', 'route_enabled', 'activated')) {
        if ($installation[$name] -ne $false) { Stop-Qualification 'BUILD_MANIFEST_HAS_INSTALL_EFFECTS' }
    }
    $hostTripleMatches = [System.Text.RegularExpressions.Regex]::Matches([string]$manifest.toolchain.rustc_version_verbose, '(?m)^host:\s*(?<triple>[A-Za-z0-9_-]+)\s*$')
    if ($hostTripleMatches.Count -ne 1) { Stop-Qualification 'BUILD_MANIFEST_TARGET_TRIPLE_INVALID' }
    $targetDirCanonical = [System.IO.Path]::GetFullPath([string]$manifest.build.target_dir).ToUpperInvariant()
    $targetDirIdentity = [Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData([System.Text.Encoding]::UTF8.GetBytes($targetDirCanonical))).ToLowerInvariant()
    return [ordered]@{
        manifest_sha256 = $manifestHash
        source_commit = $manifest.source.commit
        source_tree = $manifest.source.tree
        package_name = $manifest.build.package_name
        package_manifest = $manifest.build.package_manifest
        profile = $manifest.build.profile
        target_dir_sha256 = $targetDirIdentity
        binary_target = $ExpectedTarget
        binary_sha256 = $binaryHash
        binary_bytes = [long]$artifacts[0].bytes
        cargo_toml_sha256 = $manifest.source.cargo_toml_sha256
        cargo_lock_sha256 = $manifest.source.cargo_lock_sha256
        rust_toolchain_toml_sha256 = $manifest.source.rust_toolchain_toml_sha256
        active_toolchain = $manifest.toolchain.active_toolchain
        pinned_channel = $manifest.toolchain.pinned_channel
        rustc_version_verbose = $manifest.toolchain.rustc_version_verbose
        cargo_version_verbose = $manifest.toolchain.cargo_version_verbose
        rustc_host_triple = $hostTripleMatches[0].Groups['triple'].Value
        provenance = 'manifest_sha256_and_image_hash_verified; manifest_is_not_signed'
    }
}

function Get-PublicCliBuildProvenance {
    param(
        [Parameter(Mandatory)][string] $ManifestPath,
        [Parameter(Mandatory)][string] $ExpectedManifestSha256,
        [Parameter(Mandatory)][string] $BinaryPath,
        [Parameter(Mandatory)][string] $ExpectedBinarySha256
    )
    $manifestFile = Assert-ExistingFile $ManifestPath
    $binary = Assert-ExistingFile $BinaryPath
    if ([System.IO.Path]::GetFileName($manifestFile) -cne 'build-manifest.json') { Stop-Qualification 'PUBLIC_CLI_MANIFEST_FILENAME_INVALID' }
    $manifestHash = Get-FileSha256 $manifestFile
    $binaryHash = Get-FileSha256 $binary
    if ($manifestHash -cne $ExpectedManifestSha256.ToLowerInvariant()) { Stop-Qualification 'PUBLIC_CLI_MANIFEST_HASH_MISMATCH' }
    if ($binaryHash -cne $ExpectedBinarySha256.ToLowerInvariant()) { Stop-Qualification 'PUBLIC_CLI_BINARY_HASH_MISMATCH' }
    $manifest = Read-JsonFile $manifestFile
    if ($manifest.schema_version -ne 1 -or $manifest.format -cne 'eliot.frontend_build_manifest.v1' -or
        $manifest.source.checkout_clean_before -ne $true -or $manifest.source.checkout_clean_after -ne $true -or
        $manifest.source.commit -notmatch '\A[a-f0-9]{40}\z' -or $manifest.source.tree -notmatch '\A[a-f0-9]{40}\z' -or
        $manifest.source.cargo_toml_sha256 -notmatch '\A[a-f0-9]{64}\z' -or
        $manifest.source.cargo_lock_sha256 -notmatch '\A[a-f0-9]{64}\z' -or
        $manifest.source.rust_toolchain_toml_sha256 -notmatch '\A[a-f0-9]{64}\z' -or
        $manifest.build.role -cne 'cli_client' -or $manifest.build.profile -cne 'release' -or
        $manifest.build.package_name -cne 'swarm-cli' -or
        $manifest.build.package_manifest -cne 'crates/swarm-cli/Cargo.toml' -or
        $manifest.build.binary_target -cne 'swarm' -or
        [string]::IsNullOrWhiteSpace([string]$manifest.build.external_target_dir) -or
        -not [System.IO.Path]::IsPathFullyQualified([string]$manifest.build.external_target_dir) -or
        [string]::IsNullOrWhiteSpace([string]$manifest.toolchain.pinned_channel) -or
        [string]::IsNullOrWhiteSpace([string]$manifest.toolchain.active_toolchain) -or
        [string]::IsNullOrWhiteSpace([string]$manifest.toolchain.rustc_version_verbose) -or
        [string]::IsNullOrWhiteSpace([string]$manifest.toolchain.cargo_version_verbose)) {
        Stop-Qualification 'PUBLIC_CLI_MANIFEST_CONTRACT_INVALID'
    }
    $arguments = @($manifest.build.cargo_arguments | ForEach-Object { [string]$_ })
    foreach ($requiredArgument in @('--release', '--locked')) {
        if ($requiredArgument -notin $arguments) { Stop-Qualification 'PUBLIC_CLI_MANIFEST_ARGUMENTS_MISMATCH' }
    }
    $argumentPairs = [ordered]@{
        '--package' = 'swarm-cli'
        '--bin' = 'swarm'
        '--target-dir' = [string]$manifest.build.external_target_dir
    }
    foreach ($option in $argumentPairs.Keys) {
        $optionIndex = [Array]::IndexOf($arguments, [string]$option)
        if ($optionIndex -lt 0 -or $optionIndex + 1 -ge $arguments.Count -or $arguments[$optionIndex + 1] -cne [string]$argumentPairs[$option]) {
            Stop-Qualification 'PUBLIC_CLI_MANIFEST_ARGUMENTS_MISMATCH'
        }
    }
    $launcher = $manifest.compatibility.host_launcher
    $protocolVersion = $manifest.compatibility.host_ipc.protocol_version
    $targetTriple = [string]$manifest.compatibility.target.rustc_host_triple
    if ($null -eq $launcher -or $launcher.package_name -cne 'eliot-swarm-controller' -or
        $launcher.binary_target -cne 'swarm-host' -or @($launcher.required_arguments).Count -ne 0 -or
        $null -eq $protocolVersion -or [int]$protocolVersion -le 0 -or
        [string]::IsNullOrWhiteSpace($targetTriple)) {
        Stop-Qualification 'PUBLIC_CLI_HOST_LAUNCHER_CONTRACT_INVALID'
    }
    $artifacts = @($manifest.artifacts)
    if ($artifacts.Count -ne 1 -or $artifacts[0].target_name -cne 'swarm' -or
        $artifacts[0].file -cne 'bin/swarm.exe' -or
        [long]$artifacts[0].bytes -ne (Get-Item -LiteralPath $binary).Length -or
        $artifacts[0].source_sha256 -cne $binaryHash -or $artifacts[0].artifact_sha256 -cne $binaryHash) {
        Stop-Qualification 'PUBLIC_CLI_ARTIFACT_IDENTITY_MISMATCH'
    }
    $builtBinary = Assert-ExistingFile (Join-Path (Split-Path -LiteralPath $manifestFile -Parent) 'bin\swarm.exe')
    if ((Get-FileSha256 $builtBinary) -cne $binaryHash -or
        (Get-Item -LiteralPath $builtBinary).Length -ne (Get-Item -LiteralPath $binary).Length) {
        Stop-Qualification 'PUBLIC_CLI_IMAGE_IS_NOT_BUILD_OUTPUT'
    }
    foreach ($name in @('installed', 'registered', 'configured', 'activated')) {
        if ($manifest.installation[$name] -ne $false) { Stop-Qualification 'PUBLIC_CLI_MANIFEST_HAS_INSTALL_EFFECTS' }
    }
    $targetDirCanonical = [System.IO.Path]::GetFullPath([string]$manifest.build.external_target_dir).ToUpperInvariant()
    $targetDirIdentity = [Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData([System.Text.Encoding]::UTF8.GetBytes($targetDirCanonical))).ToLowerInvariant()
    return [ordered]@{
        manifest_sha256 = $manifestHash
        source_commit = $manifest.source.commit
        source_tree = $manifest.source.tree
        package_name = $manifest.build.package_name
        package_manifest = $manifest.build.package_manifest
        profile = $manifest.build.profile
        target_dir_sha256 = $targetDirIdentity
        binary_target = 'swarm'
        binary_sha256 = $binaryHash
        binary_bytes = [long]$artifacts[0].bytes
        cargo_toml_sha256 = $manifest.source.cargo_toml_sha256
        cargo_lock_sha256 = $manifest.source.cargo_lock_sha256
        rust_toolchain_toml_sha256 = $manifest.source.rust_toolchain_toml_sha256
        active_toolchain = $manifest.toolchain.active_toolchain
        pinned_channel = $manifest.toolchain.pinned_channel
        rustc_version_verbose = $manifest.toolchain.rustc_version_verbose
        cargo_version_verbose = $manifest.toolchain.cargo_version_verbose
        host_launcher_package_name = $launcher.package_name
        host_launcher_binary_target = $launcher.binary_target
        host_ipc_protocol_version = [int]$protocolVersion
        rustc_host_triple = $targetTriple
        provenance = 'frontend_manifest_sha256_and_image_hash_verified; manifest_is_not_signed'
    }
}

function Assert-PublicCliHostSibling {
    param(
        [Parameter(Mandatory)][string] $PublicCliPath,
        [Parameter(Mandatory)][string] $HostPath,
        [Parameter(Mandatory)][System.Collections.IDictionary] $PublicCliBuild,
        [Parameter(Mandatory)][System.Collections.IDictionary] $HostBuild
    )
    $expectedSibling = [System.IO.Path]::GetFullPath((Join-Path (Split-Path -LiteralPath $PublicCliPath -Parent) 'swarm-host.exe'))
    if ([System.IO.Path]::GetFileName($PublicCliPath) -cne 'swarm.exe' -or
        -not [string]::Equals($expectedSibling, $HostPath, [StringComparison]::OrdinalIgnoreCase) -or
        $PublicCliBuild.host_launcher_package_name -cne $HostBuild.package_name -or
        $PublicCliBuild.host_launcher_binary_target -cne $HostBuild.binary_target -or
        $PublicCliBuild.rustc_host_triple -cne $HostBuild.rustc_host_triple -or
        (Get-FileSha256 $expectedSibling) -cne $HostBuild.binary_sha256) {
        Stop-Qualification 'PUBLIC_CLI_HOST_COORDINATE_MISMATCH'
    }
}

function New-PrivateRunDirectory {
    param([Parameter(Mandatory)][string] $Parent)
    $runId = [Guid]::NewGuid().ToString('D')
    $path = Join-Path $Parent $runId
    $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    $security = [System.Security.AccessControl.DirectorySecurity]::new()
    $security.SetAccessRuleProtection($true, $false)
    $security.SetOwner($identity.User)
    $rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
        $identity.User,
        [System.Security.AccessControl.FileSystemRights]::FullControl,
        ([System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [System.Security.AccessControl.InheritanceFlags]::ObjectInherit),
        [System.Security.AccessControl.PropagationFlags]::None,
        [System.Security.AccessControl.AccessControlType]::Allow
    )
    [void]$security.AddAccessRule($rule)
    [void][System.IO.Directory]::CreateDirectory($path, $security)
    [void](Assert-NoReparseTraversal $path)
    $script:RunDirectory = $path
    $script:RequestDirectory = Join-Path $path 'requests'
    $script:StateDirectory = Join-Path $path 'state'
    [void][System.IO.Directory]::CreateDirectory($script:RequestDirectory)
    [void][System.IO.Directory]::CreateDirectory($script:StateDirectory)
    $script:Report.run_id = $runId
    Save-Report
}

function Save-Report {
    if ([string]::IsNullOrWhiteSpace($script:RunDirectory)) { return }
    $script:Report.stages = @($script:Stages.ToArray())
    $bytes = [System.Text.UTF8Encoding]::new($false).GetBytes(($script:Report | ConvertTo-Json -Depth 32))
    $target = Join-Path $script:RunDirectory 'qualification-receipt.json'
    $temporary = Join-Path $script:RunDirectory ('.receipt-' + [Guid]::NewGuid().ToString('N') + '.tmp')
    $stream = [System.IO.File]::Open($temporary, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
    try {
        $stream.Write($bytes, 0, $bytes.Length)
        $stream.Flush($true)
    }
    finally { $stream.Dispose() }
    [System.IO.File]::Move($temporary, $target, $true)
}

function Write-PrivateJson {
    param([Parameter(Mandatory)][string] $Path, [Parameter(Mandatory)] $Value)
    $bytes = [System.Text.UTF8Encoding]::new($false).GetBytes(($Value | ConvertTo-Json -Depth 48))
    $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
    try {
        $stream.Write($bytes, 0, $bytes.Length)
        $stream.Flush($true)
    }
    finally { $stream.Dispose() }
}

function Read-JsonFile {
    param([Parameter(Mandatory)][string] $Path)
    try { return (Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json -AsHashtable -Depth 48) }
    catch { Stop-Qualification 'JSON_INPUT_INVALID' }
}

function Test-ExactObjectKeys {
    param([AllowNull()] $Value, [Parameter(Mandatory)][string[]] $ExpectedKeys)
    if (-not ($Value -is [System.Collections.IDictionary])) { return $false }
    $actual = @($Value.Keys | ForEach-Object { [string]$_ } | Sort-Object -CaseSensitive)
    $expected = @($ExpectedKeys | Sort-Object -CaseSensitive)
    return (($actual -join ',') -ceq ($expected -join ','))
}

function Get-OptionalField {
    param([AllowNull()] $Object, [Parameter(Mandatory)][string] $Name)
    if ($null -eq $Object) { return $null }
    if ($Object -is [System.Collections.IDictionary]) { return $Object[$Name] }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -ne $property) { return $property.Value }
    return $null
}

function New-RequestId {
    return 'native-qualification-' + [Guid]::NewGuid().ToString('N')
}

function Get-SafeErrorCode {
    param([AllowNull()][string] $Text)
    if ([string]::IsNullOrWhiteSpace($Text)) { return 'CLI_EXIT' }
    try {
        $value = $Text | ConvertFrom-Json -AsHashtable -Depth 16
        $code = $value.error.data.code
        if ([string]::IsNullOrWhiteSpace($code)) { $code = $value.error.code }
        if ($code -is [string] -and $code -match '\A[A-Z0-9_]{1,64}\z') { return $code }
    }
    catch { }
    return 'CLI_EXIT'
}

function Invoke-SwarmProcess {
    param(
        [Parameter(Mandatory)][string[]] $Arguments,
        [Parameter(Mandatory)][int] $TimeoutMilliseconds,
        [switch] $HostProcess
    )
    $start = [System.Diagnostics.ProcessStartInfo]::new()
    $executablePath = if ($HostProcess) { $script:HostPath } else { $script:PublicCliPath }
    if ([string]::IsNullOrWhiteSpace([string]$executablePath)) { Stop-Qualification 'QUALIFICATION_BINARY_PATH_UNSET' }
    $start.FileName = $executablePath
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.RedirectStandardInput = [bool]$HostProcess
    $start.WorkingDirectory = Split-Path -LiteralPath $executablePath -Parent
    foreach ($argument in $Arguments) { [void]$start.ArgumentList.Add([string]$argument) }
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $start
    try {
        if (-not $process.Start()) { return [pscustomobject]@{ started = $false; completed = $true; exit_code = $null; error_code = 'PROCESS_START_FAILED'; stdout = $null; process_id = $null } }
        $processId = $process.Id
        if ($HostProcess) {
            $process.add_OutputDataReceived([System.Diagnostics.DataReceivedEventHandler]{ param($sender, $eventArgs) })
            $process.add_ErrorDataReceived([System.Diagnostics.DataReceivedEventHandler]{ param($sender, $eventArgs) })
            $process.BeginOutputReadLine()
            $process.BeginErrorReadLine()
            return [pscustomobject]@{ started = $true; completed = $false; exit_code = $null; error_code = $null; stdout = $null; process_id = $processId; process = $process }
        }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        $completed = $process.WaitForExit($TimeoutMilliseconds)
        if (-not $completed) {
            $script:PublicCliProcessIdsPending.Add($processId)
            return [pscustomobject]@{ started = $true; completed = $false; exit_code = $null; error_code = 'CLI_TIMEOUT_OUTCOME_UNKNOWN'; stdout = $null; process_id = $processId }
        }
        $process.WaitForExit()
        $stdout = $stdoutTask.GetAwaiter().GetResult()
        $stderr = $stderrTask.GetAwaiter().GetResult()
        if ($stdout.Length -gt $script:MaxCliOutputCharacters) {
            return [pscustomobject]@{ started = $true; completed = $true; exit_code = $process.ExitCode; error_code = 'CLI_OUTPUT_LIMIT'; stdout = $null; process_id = $processId }
        }
        $safeCode = if ($process.ExitCode -eq 0) { $null } else { Get-SafeErrorCode $stderr }
        return [pscustomobject]@{ started = $true; completed = $true; exit_code = $process.ExitCode; error_code = $safeCode; stdout = $stdout; process_id = $processId }
    }
    catch {
        return [pscustomobject]@{ started = $false; completed = $true; exit_code = $null; error_code = 'PROCESS_IO_FAILED'; stdout = $null; process_id = $null }
    }
    finally {
        if (-not $HostProcess -or $process.HasExited) { $process.Dispose() }
    }
}

function Invoke-ApplicationCall {
    param(
        [Parameter(Mandatory)][string] $CredentialPath,
        [Parameter(Mandatory)][string] $Method,
        [Parameter(Mandatory)][System.Collections.IDictionary] $Params,
        [Parameter(Mandatory)][int] $TimeoutMilliseconds
    )
    $requestId = if ($Params.Contains('client_request_id')) { [string]$Params.client_request_id } else { $null }
    $fileName = ([Guid]::NewGuid().ToString('N') + '.json')
    $requestPath = Join-Path $script:RequestDirectory $fileName
    Write-PrivateJson -Path $requestPath -Value $Params
    $arguments = @('--config', $script:ConfigPath, '--data-dir', $script:StateDirectory, '--credential', $CredentialPath, 'call', $Method, '--file', $requestPath)
    $result = Invoke-SwarmProcess -Arguments $arguments -TimeoutMilliseconds $TimeoutMilliseconds
    if ($result.completed) {
        try { [System.IO.File]::Delete($requestPath) } catch { }
    }
    elseif ($null -ne $result.process_id) {
        $script:TimedOutRequestFiles.Add([pscustomobject]@{ public_cli_process_id = [int]$result.process_id; path = $requestPath })
    }
    $parsed = $null
    if ($result.completed -and $result.exit_code -eq 0 -and $null -ne $result.stdout) {
        try { $parsed = $result.stdout | ConvertFrom-Json -AsHashtable -Depth 48 }
        catch { $result.error_code = 'CLI_RESULT_INVALID' }
    }
    return [pscustomobject]@{
        completed = $result.completed
        exit_code = $result.exit_code
        error_code = $result.error_code
        value = $parsed
        request_id = $requestId
        public_cli_process_id = $result.process_id
    }
}

function Invoke-ManagerCall {
    param([string] $Method, [System.Collections.IDictionary] $Params, [int] $TimeoutMilliseconds = 30000)
    return Invoke-ApplicationCall -CredentialPath $script:ManagerCredentialPath -Method $Method -Params $Params -TimeoutMilliseconds $TimeoutMilliseconds
}

function Get-AdapterContract {
    switch ($Adapter) {
        'OpenCode' {
            return [pscustomobject]@{
                module_id = 'eliot.opencode.v2'; artifact_id = 'eliot-opencode-v2.rust-http.1'; version = '0.3.0'; runtime = 'module';
                build_package = 'swarm-adapter-opencode'; build_target = 'swarm-adapter-opencode';
                capabilities = @('agent.open', 'agent.reconcile', 'agent.result', 'agent.send/next_turn', 'task.dispatch', 'native.mcp.arm', 'native.mcp.install', 'native.mcp.observe', 'native.mcp.read');
                command_schemas = @('swarm.native_mcp_command@1:', 'swarm.normalized_result_context@1:', 'swarm.runtime_command@1:', 'swarm.task_dispatch_context@1:');
                event_schemas = @('swarm.normalized_result_page@1:', 'swarm.runtime_outcome@1:', 'swarm.task_dispatch_admission@1:');
                expected_config_schema_id = 'opencode-v2-native-options'; expected_config_schema_version = '2'; expected_config_schema_sha256 = 'a43c9b7284dba6efd087c04a747d74ef3bd5ce7a648697f160346a8be5265d9d';
                model_provider_field = 'providerID'; model_field = 'id'; effort_field = 'variant';
                expected_provider = $null; expected_model = $null; expected_model_ref = $OpenCodeCommandTestModelRef; expected_effort = $null
            }
        }
        'Command' {
            return [pscustomobject]@{
                module_id = 'runtime.command'; artifact_id = 'eliot-command.rust-headless.1'; version = '3'; runtime = 'command';
                build_package = 'swarm-adapter-command'; build_target = 'swarm-adapter-command';
                capabilities = @('agent.open', 'agent.reconcile', 'agent.refresh', 'agent.result', 'task.dispatch');
                command_schemas = @('swarm.normalized_result_context@1:', 'swarm.runtime_command@1:', 'swarm.task_dispatch_context@1:');
                event_schemas = @('swarm.normalized_result_page@1:', 'swarm.runtime_outcome@1:', 'swarm.task_dispatch_admission@1:');
                expected_config_schema_id = $null; expected_config_schema_version = $null; expected_config_schema_sha256 = $null;
                model_provider_field = $null; model_field = 'modelId'; effort_field = $null;
                expected_provider = $null; expected_model = $OpenCodeCommandTestModelRef; expected_model_ref = $null; expected_effort = $null
            }
        }
        'Codex' {
            return [pscustomobject]@{
                module_id = 'codex'; artifact_id = 'codex-rust-controller.1'; version = '4'; runtime = 'codex';
                build_package = 'swarm-adapter-codex'; build_target = 'swarm-codex-adapter';
                capabilities = @('agent.open', 'agent.reconcile', 'agent.result', 'agent.send', 'task.dispatch');
                command_schemas = @('swarm.normalized_result_context@1:', 'swarm.runtime_command@1:', 'swarm.task_dispatch_context@1:');
                event_schemas = @('swarm.normalized_result_page@1:', 'swarm.runtime_outcome@1:', 'swarm.task_dispatch_admission@1:');
                expected_config_schema_id = $null; expected_config_schema_version = $null; expected_config_schema_sha256 = $null;
                model_provider_field = 'modelProvider'; model_field = 'model'; effort_field = $null;
                expected_provider = $null; expected_model = $null; expected_model_ref = $null; expected_effort = $null
            }
        }
        'Antigravity' {
            return [pscustomobject]@{
                module_id = 'antigravity'; artifact_id = 'eliot-antigravity.rust-headless.1'; version = '4'; runtime = 'antigravity';
                build_package = 'swarm-antigravity-adapter'; build_target = 'swarm-antigravity';
                capabilities = @('agent.open', 'agent.reconcile', 'agent.refresh', 'agent.result', 'agent.send/next_turn', 'task.dispatch');
                command_schemas = @('swarm.normalized_result_context@1:', 'swarm.runtime_command@1:', 'swarm.task_dispatch_context@1:');
                event_schemas = @('swarm.normalized_result_page@1:', 'swarm.runtime_outcome@1:', 'swarm.task_dispatch_admission@1:');
                expected_config_schema_id = $null; expected_config_schema_version = $null; expected_config_schema_sha256 = $null;
                model_provider_field = $null; model_field = 'modelId'; effort_field = $null;
                expected_provider = $null; expected_model = 'gemini-3.8-flash-high'; expected_model_ref = $null; expected_effort = $null
            }
        }
        'Claude' {
            return [pscustomobject]@{
                module_id = 'claude'; artifact_id = 'claude-agent-sdk-0.3.287-rust-controller.4'; version = '4'; runtime = 'claude';
                build_package = 'swarm-adapter-claude'; build_target = 'swarm-adapter-claude';
                capabilities = @('agent.open', 'agent.reconcile', 'agent.result', 'agent.refresh', 'agent.send/next_turn', 'task.dispatch');
                command_schemas = @('swarm.runtime_command@1:', 'swarm.task_dispatch_context@1:');
                event_schemas = @('swarm.runtime_outcome@1:', 'swarm.task_dispatch_admission@1:');
                expected_config_schema_id = $null; expected_config_schema_version = $null; expected_config_schema_sha256 = $null;
                model_provider_field = $null; model_field = 'modelId'; effort_field = $null;
                expected_provider = $null; expected_model = $null; expected_model_ref = $null; expected_effort = $null
            }
        }
        default { Stop-Qualification 'CLAUDE_ADAPTER_UNAVAILABLE' }
    }
}

function Assert-TaskFixture {
    param([Parameter(Mandatory)][System.Collections.IDictionary] $Spec)
    if (-not ($Spec.objective -is [string]) -or [string]::IsNullOrWhiteSpace($Spec.objective) -or
        -not ($Spec.phase -is [string]) -or [string]::IsNullOrWhiteSpace($Spec.phase) -or
        -not ($Spec.requirements -is [System.Collections.IList]) -or $Spec.requirements.Count -lt 1 -or
        -not ($Spec.scope -is [System.Collections.IDictionary]) -or
        -not ($Spec.scope.initial_paths -is [System.Collections.IList]) -or
        $Spec.scope.initial_paths.Count -lt 1) {
        Stop-Qualification 'TASK_SPEC_FIXTURE_INCOMPLETE'
    }
    foreach ($requirement in $Spec.requirements) {
        if (-not ($requirement -is [System.Collections.IDictionary]) -or
            [string]::IsNullOrWhiteSpace([string]$requirement.id) -or
            [string]::IsNullOrWhiteSpace([string]$requirement.statement)) {
            Stop-Qualification 'TASK_SPEC_FIXTURE_INCOMPLETE'
        }
    }
    foreach ($path in $Spec.scope.initial_paths) {
        if (-not ($path -is [string]) -or [string]::IsNullOrWhiteSpace($path)) { Stop-Qualification 'TASK_SPEC_FIXTURE_INCOMPLETE' }
    }
}

function Assert-LaunchSettings {
    param([Parameter(Mandatory)][System.Collections.IDictionary] $Settings)
    $required = @('agent_profile', 'mcp_profile', 'mcp_surface', 'workspace_policy', 'requested_model', 'requested_effort', 'budget', 'stop_conditions', 'purpose')
    foreach ($key in $required) { if (-not $Settings.Contains($key)) { Stop-Qualification 'LAUNCH_SETTINGS_INCOMPLETE' } }
    foreach ($key in $Settings.Keys) { if ($key -notin $required) { Stop-Qualification 'LAUNCH_SETTINGS_UNKNOWN_FIELD' } }
    foreach ($key in @('agent_profile', 'mcp_profile', 'mcp_surface', 'workspace_policy', 'purpose')) {
        if (-not ($Settings[$key] -is [string]) -or [string]::IsNullOrWhiteSpace($Settings[$key])) { Stop-Qualification 'LAUNCH_SETTINGS_INCOMPLETE' }
    }
    if ($Settings.workspace_policy -ne 'manager_owned_worktree') { Stop-Qualification 'WORKSPACE_POLICY_MUST_BE_EXPLICIT_MANAGER_WORKTREE' }
    foreach ($key in @('requested_model', 'requested_effort')) {
        if ($null -ne $Settings[$key] -and -not ($Settings[$key] -is [string])) { Stop-Qualification 'LAUNCH_SETTINGS_INVALID' }
    }
    if (-not ($Settings.budget -is [System.Collections.IDictionary])) { Stop-Qualification 'LAUNCH_SETTINGS_INVALID' }
    $budgetKeys = @('max_turns', 'max_duration_ms', 'max_cost_units')
    if (@($Settings.budget.Keys | Sort-Object) -join ',' -ne (@($budgetKeys | Sort-Object) -join ',')) { Stop-Qualification 'LAUNCH_SETTINGS_INVALID' }
    foreach ($key in $budgetKeys) {
        $value = $Settings.budget[$key]
        if ($null -ne $value -and (($value -isnot [long] -and $value -isnot [int] -and $value -isnot [System.Numerics.BigInteger]) -or [decimal]$value -lt 0)) {
            Stop-Qualification 'LAUNCH_SETTINGS_INVALID'
        }
    }
    if (-not ($Settings.stop_conditions -is [System.Collections.IList]) -or $Settings.stop_conditions.Count -gt 16) { Stop-Qualification 'LAUNCH_SETTINGS_INVALID' }
    foreach ($condition in $Settings.stop_conditions) {
        if (-not ($condition -is [string]) -or [string]::IsNullOrWhiteSpace($condition)) { Stop-Qualification 'LAUNCH_SETTINGS_INVALID' }
    }
}

function Get-TomlSectionText {
    param([Parameter(Mandatory)][string] $Text, [Parameter(Mandatory)][string] $Section)
    $escaped = [regex]::Escape($Section)
    $match = [regex]::Match($Text, '(?ms)^\s*\[' + $escaped + '\]\s*\r?\n(?<body>.*?)(?=^\s*\[|\z)')
    if (-not $match.Success) { Stop-Qualification 'MODULE_SUPERVISOR_CONFIG_MISSING' }
    return $match.Groups['body'].Value
}

function Get-TomlStringValue {
    param([Parameter(Mandatory)][string] $SectionText, [Parameter(Mandatory)][string] $Name)
    $match = [regex]::Match($SectionText, '(?m)^\s*' + [regex]::Escape($Name) + '\s*=\s*(?<value>"(?:\\.|[^"\\])*"|''[^'']*'')\s*(?:#.*)?$')
    if (-not $match.Success) { Stop-Qualification 'MODULE_SUPERVISOR_CONFIG_INCOMPLETE' }
    $literal = $match.Groups['value'].Value
    if ($literal.StartsWith('"')) {
        try { return [string]($literal | ConvertFrom-Json -AsHashtable) }
        catch { Stop-Qualification 'MODULE_SUPERVISOR_CONFIG_INVALID' }
    }
    return $literal.Substring(1, $literal.Length - 2)
}

function Get-TomlStringArray {
    param([Parameter(Mandatory)][string] $SectionText, [Parameter(Mandatory)][string] $Name)
    $match = [regex]::Match($SectionText, '(?ms)^\s*' + [regex]::Escape($Name) + '\s*=\s*\[(?<body>.*?)\]\s*(?:#.*)?$')
    if (-not $match.Success) { Stop-Qualification 'MODULE_SUPERVISOR_CONFIG_INCOMPLETE' }
    $values = [System.Collections.Generic.List[string]]::new()
    foreach ($stringMatch in [regex]::Matches($match.Groups['body'].Value, '"(?:\\.|[^"\\])*"|''[^'']*''')) {
        $literal = $stringMatch.Value
        if ($literal.StartsWith('"')) {
            try { $values.Add([string]($literal | ConvertFrom-Json -AsHashtable)) }
            catch { Stop-Qualification 'MODULE_SUPERVISOR_CONFIG_INVALID' }
        }
        else { $values.Add($literal.Substring(1, $literal.Length - 2)) }
    }
    return @($values.ToArray())
}

function Assert-ModuleSupervisorConfig {
    param([Parameter(Mandatory)][string] $Path, [Parameter(Mandatory)][string] $InstallRoot,
        [Parameter(Mandatory)][string] $DescriptorPath, [Parameter(Mandatory)][string] $OwnerHelperPath,
        [Parameter(Mandatory)][string] $OwnerHelperSha256)
    $text = Get-Content -LiteralPath $Path -Raw
    $section = Get-TomlSectionText -Text $text -Section 'module_supervisor'
    $enabled = [regex]::Match($section, '(?m)^\s*enabled\s*=\s*(true|false)\s*(?:#.*)?$')
    if (-not $enabled.Success -or $enabled.Groups[1].Value -ne 'true') { Stop-Qualification 'MODULE_SUPERVISOR_DISABLED' }
    $configuredInstallRoot = Assert-AbsolutePath (Get-TomlStringValue $section 'install_root')
    $configuredHelper = Assert-AbsolutePath (Get-TomlStringValue $section 'owner_helper')
    $configuredHelperHash = (Get-TomlStringValue $section 'owner_helper_sha256').ToLowerInvariant()
    $configuredDescriptors = @(Get-TomlStringArray $section 'descriptor_files' | ForEach-Object { Assert-AbsolutePath $_ })
    if (-not [string]::Equals($configuredInstallRoot, $InstallRoot, [StringComparison]::OrdinalIgnoreCase) -or
        -not [string]::Equals($configuredHelper, $OwnerHelperPath, [StringComparison]::OrdinalIgnoreCase) -or
        $configuredHelperHash -ne $OwnerHelperSha256.ToLowerInvariant() -or
        -not ($configuredDescriptors | Where-Object { [string]::Equals($_, $DescriptorPath, [StringComparison]::OrdinalIgnoreCase) })) {
        Stop-Qualification 'MODULE_SUPERVISOR_CONFIG_MISMATCH'
    }
}

function Get-InstalledModuleFacts {
    param([Parameter(Mandatory)][string] $InstallRoot, [Parameter(Mandatory)][string] $ExecutablePath,
        [Parameter(Mandatory)][string] $DescriptorPath, [Parameter(Mandatory)][string] $OwnerHelperPath,
        [Parameter(Mandatory)][string] $OwnerHelperSha256, [Parameter(Mandatory)] $Contract)
    $installRoot = Assert-ExistingDirectory $InstallRoot
    $exe = Assert-ExistingFile $ExecutablePath
    $descriptorFile = Assert-ExistingFile $DescriptorPath
    $helper = Assert-ExistingFile $OwnerHelperPath
    if (-not $exe.StartsWith($installRoot.TrimEnd('\') + '\', [StringComparison]::OrdinalIgnoreCase) -or
        -not $descriptorFile.StartsWith($installRoot.TrimEnd('\') + '\', [StringComparison]::OrdinalIgnoreCase)) {
        Stop-Qualification 'MODULE_ARTIFACT_OUTSIDE_INSTALL_ROOT'
    }
    $exeHash = Get-FileSha256 $exe
    $helperHash = Get-FileSha256 $helper
    if ($helperHash -ne $OwnerHelperSha256.ToLowerInvariant()) { Stop-Qualification 'MODULE_OWNER_HELPER_HASH_MISMATCH' }
    $descriptorBytes = [System.IO.File]::ReadAllBytes($descriptorFile)
    $descriptorHash = [Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData($descriptorBytes)).ToLowerInvariant()
    $descriptor = Read-JsonFile $descriptorFile
    if ($descriptor.schema_version -ne 1 -or $descriptor.module_id -cne $Contract.module_id -or
        $descriptor.artifact.artifact_id -cne $Contract.artifact_id -or $descriptor.artifact.version -cne $Contract.version -or
        $descriptor.enabled -ne $true -or $descriptor.launch.executable -ne $exe -or
        $descriptor.launch.executable_sha256 -ne $exeHash) {
        Stop-Qualification 'MODULE_DESCRIPTOR_IDENTITY_MISMATCH'
    }
    if ($descriptor.protocol.minimum.major -gt 1 -or $descriptor.protocol.maximum.major -lt 1 -or
        ($descriptor.protocol.minimum.major -eq 1 -and $descriptor.protocol.minimum.minor -gt 0) -or
        ($descriptor.protocol.maximum.major -eq 1 -and $descriptor.protocol.maximum.minor -lt 0)) {
        Stop-Qualification 'MODULE_PROTOCOL_INCOMPATIBLE'
    }
    if ($null -eq $Contract.expected_config_schema_id) {
        if ($null -ne $descriptor.config_schema) { Stop-Qualification 'MODULE_CONFIG_SCHEMA_MISMATCH' }
    }
    else {
        $configSchema = $descriptor.config_schema
        if ($null -eq $configSchema) { Stop-Qualification 'MODULE_CONFIG_SCHEMA_MISMATCH' }
        $configSchemaSha256 = [string]$configSchema.sha256
        if ($configSchema.schema_id -cne $Contract.expected_config_schema_id -or
            $configSchema.version -cne $Contract.expected_config_schema_version -or
            $configSchemaSha256 -cnotmatch '^[a-f0-9]{64}$' -or
            ($null -ne $Contract.expected_config_schema_sha256 -and
                $configSchemaSha256 -cne $Contract.expected_config_schema_sha256)) {
            Stop-Qualification 'MODULE_CONFIG_SCHEMA_MISMATCH'
        }
    }
    $commandSchemas = @($descriptor.command_schemas | ForEach-Object { "{0}@{1}:{2}" -f $_.schema_id, $_.version, $_.sha256 } | Sort-Object)
    $eventSchemas = @($descriptor.event_schemas | ForEach-Object { "{0}@{1}:{2}" -f $_.schema_id, $_.version, $_.sha256 } | Sort-Object)
    $expectedCommandSchemas = @($Contract.command_schemas | Sort-Object)
    $expectedEventSchemas = @($Contract.event_schemas | Sort-Object)
    if (($commandSchemas -join ',') -cne ($expectedCommandSchemas -join ',') -or
        ($eventSchemas -join ',') -cne ($expectedEventSchemas -join ',')) {
        Stop-Qualification 'MODULE_SCHEMA_SET_MISMATCH'
    }
    $capabilities = @($descriptor.capabilities | ForEach-Object { [string]$_ } | Sort-Object)
    $expectedCapabilities = @($Contract.capabilities | Sort-Object)
    if (($capabilities -join ',') -cne ($expectedCapabilities -join ',')) { Stop-Qualification 'MODULE_CAPABILITY_SET_MISMATCH' }
    if ($descriptor.lifecycle -notin @('external_attach', 'owned_service') -or $descriptor.activation -ne 'on_demand') {
        Stop-Qualification 'MODULE_LIFECYCLE_UNSUPPORTED'
    }
    $parent = Split-Path -LiteralPath $descriptorFile -Parent
    $receiptPath = Join-Path $parent 'install-receipt.json'
    $coordinatePath = Join-Path $parent 'install-coordinate.json'
    [void](Assert-ExistingFile $receiptPath)
    [void](Assert-ExistingFile $coordinatePath)
    $receiptBytes = [System.IO.File]::ReadAllBytes($receiptPath)
    $receiptHash = [Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData($receiptBytes)).ToLowerInvariant()
    $receipt = Read-JsonFile $receiptPath
    $coordinate = Read-JsonFile $coordinatePath
    if ($receipt.schema_version -ne 1 -or $receipt.format -ne 'eliot.module_install_receipt.v1' -or
        $receipt.module_id -ne $Contract.module_id -or $receipt.artifact_id -ne $Contract.artifact_id -or
        $receipt.version -ne $Contract.version -or $receipt.installed_file -ne $exe -or
        $receipt.descriptor_file -ne $descriptorFile -or $receipt.installed_sha256 -ne $exeHash -or
        $receipt.staged_sha256 -ne $exeHash -or $receipt.source_sha256 -ne $exeHash -or
        $receipt.descriptor_sha256 -ne $descriptorHash) {
        Stop-Qualification 'MODULE_INSTALL_RECEIPT_MISMATCH'
    }
    if ($coordinate.schema_version -ne 1 -or $coordinate.format -ne 'eliot.module_install_coordinate.v1' -or
        $coordinate.module_id -ne $Contract.module_id -or $coordinate.artifact_id -ne $Contract.artifact_id -or
        $coordinate.version -ne $Contract.version -or $coordinate.source_file -ne $receipt.source_file -or
        $coordinate.source_sha256 -ne $exeHash -or $coordinate.descriptor_sha256 -ne $descriptorHash -or
        $coordinate.receipt_sha256 -ne $receiptHash) {
        Stop-Qualification 'MODULE_INSTALL_COORDINATE_MISMATCH'
    }
    return [pscustomobject]@{
        install_root = $installRoot; executable = $exe; executable_sha256 = $exeHash
        descriptor = $descriptor; descriptor_path = $descriptorFile; descriptor_sha256 = $descriptorHash
        receipt_sha256 = $receiptHash; owner_helper = $helper; owner_helper_sha256 = $helperHash
        config_path = $null
    }
}

function Get-AdapterConfigPath {
    param([Parameter(Mandatory)] $Descriptor)
    $arguments = @($Descriptor.launch.argv)
    if ($Descriptor.module_id -ceq 'runtime.command') {
        if ($arguments.Count -ne 4 -or
            -not (Test-ExactObjectKeys -Value $arguments[0] -ExpectedKeys @('kind', 'value')) -or
            -not (Test-ExactObjectKeys -Value $arguments[1] -ExpectedKeys @('kind', 'value')) -or
            -not (Test-ExactObjectKeys -Value $arguments[2] -ExpectedKeys @('kind', 'value')) -or
            -not (Test-ExactObjectKeys -Value $arguments[3] -ExpectedKeys @('kind', 'value')) -or
            $arguments[0].kind -cne 'literal' -or $arguments[0].value -cne '--module-host-config' -or
            $arguments[1].kind -cne 'module_host_config_path' -or
            -not (Test-ExactObjectKeys -Value $arguments[1].value -ExpectedKeys @('schema_version')) -or
            $arguments[1].value.schema_version -isnot [int] -or $arguments[1].value.schema_version -ne 1 -or
            $arguments[2].kind -cne 'literal' -or $arguments[2].value -cne '--config' -or
            $arguments[3].kind -cne 'literal' -or -not ($arguments[3].value -is [string]) -or
            -not [System.IO.Path]::IsPathFullyQualified([string]$arguments[3].value)) {
            Stop-Qualification 'COMMAND_DESCRIPTOR_LAUNCH_CONTRACT_MISMATCH'
        }
        return (Assert-ExistingFile ([string]$arguments[3].value))
    }
    for ($index = 0; $index -lt $arguments.Count; $index++) {
        $argument = $arguments[$index]
        if ($argument.kind -ne 'literal' -or -not ($argument.value -is [string])) { continue }
        if ($argument.value -in @('--config', '-c')) {
            if ($index + 1 -ge $arguments.Count -or $arguments[$index + 1].kind -ne 'literal') { Stop-Qualification 'ADAPTER_CONFIG_ARGUMENT_INVALID' }
            $candidate = [string]$arguments[$index + 1].value
            if ([System.IO.Path]::IsPathFullyQualified($candidate)) { return (Assert-ExistingFile $candidate) }
            Stop-Qualification 'ADAPTER_CONFIG_PATH_MUST_BE_ABSOLUTE'
        }
        if ([System.IO.Path]::IsPathFullyQualified([string]$argument.value) -and [System.IO.Path]::GetExtension([string]$argument.value) -in @('.json', '.toml')) {
            return (Assert-ExistingFile ([string]$argument.value))
        }
    }
    Stop-Qualification 'ADAPTER_CONFIG_ARGUMENT_UNKNOWN'
}

function Assert-CommandAdapterConfig {
    param([Parameter(Mandatory)][string] $Path)
    $config = Read-JsonFile $Path
    if (-not ($config -is [System.Collections.IDictionary])) { Stop-Qualification 'COMMAND_CONFIG_INVALID' }
    if (-not (Test-ExactObjectKeys -Value $config -ExpectedKeys @('command', 'command_args', 'mod_path', 'module_artifact_id', 'run_timeout_ms')) -or
        $config.module_artifact_id -cne 'eliot-command.rust-headless.1' -or
        -not ($config.command -is [string]) -or -not ($config.mod_path -is [string]) -or
        -not [System.IO.Path]::IsPathFullyQualified([string]$config.command) -or
        -not [System.IO.Path]::IsPathFullyQualified([string]$config.mod_path) -or
        -not ($config.command_args -is [System.Collections.IList]) -or
        $config.command_args.Count -gt 32 -or
        ($config.run_timeout_ms -isnot [int] -and $config.run_timeout_ms -isnot [long]) -or
        [long]$config.run_timeout_ms -lt 100 -or [long]$config.run_timeout_ms -gt 86400000) {
        Stop-Qualification 'COMMAND_CONFIG_INVALID'
    }
    $commandPath = Assert-ExistingFile ([string]$config.command)
    $modPath = Assert-ExistingFile ([string]$config.mod_path)
    if ([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Windows) -and
        [System.IO.Path]::GetExtension($commandPath) -in @('.cmd', '.bat')) {
        Stop-Qualification 'COMMAND_CONFIG_INVALID'
    }
    foreach ($argument in $config.command_args) {
        $hasControl = $false
        if ($argument -is [string]) {
            $hasControl = [string]$argument -match '[\x00-\x1F\x7F]'
        }
        if (-not ($argument -is [string]) -or
            -not [System.IO.Path]::IsPathFullyQualified([string]$argument) -or
            [System.Text.Encoding]::UTF8.GetByteCount([string]$argument) -gt 4096 -or
            $hasControl) {
            Stop-Qualification 'COMMAND_CONFIG_INVALID'
        }
    }
    $modItem = Get-Item -LiteralPath $modPath -Force
    if ($modItem.Length -gt 2MB) { Stop-Qualification 'COMMAND_CONFIG_INVALID' }
    $modBytes = [System.IO.File]::ReadAllBytes($modPath)
    $normalizedMod = [System.Collections.Generic.List[byte]]::new()
    for ($index = 0; $index -lt $modBytes.Length; $index++) {
        if ($modBytes[$index] -eq 13 -and $index + 1 -lt $modBytes.Length -and $modBytes[$index + 1] -eq 10) {
            $normalizedMod.Add(10)
            $index++
        }
        else { $normalizedMod.Add($modBytes[$index]) }
    }
    $modSha256 = [Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData($normalizedMod.ToArray())).ToLowerInvariant()
    if ($modSha256 -cne '513eaa7d6034cc22b5abf14d080888cdd3e8782133e39f859b7703db123e1f80') {
        Stop-Qualification 'COMMAND_MOD_PIN_MISMATCH'
    }
    return [ordered]@{
        executable_sha256 = Get-FileSha256 $commandPath
        mod_sha256 = $modSha256
        fixed_argument_count = $config.command_args.Count
        run_timeout_ms = [long]$config.run_timeout_ms
        content = 'configuration_and_file_digests_verified; native process execution is not established by this check'
    }
}

function Assert-CodexProcessAttachment {
    param([Parameter(Mandatory)][string] $AdapterConfigPath)
    if ($CodexAppServerPid -le 0 -or [string]::IsNullOrWhiteSpace($CodexAppServerImagePath)) { Stop-Qualification 'CODEX_ATTACH_PID_AND_IMAGE_REQUIRED' }
    $expectedImage = Assert-ExistingFile $CodexAppServerImagePath
    try {
        $process = [System.Diagnostics.Process]::GetProcessById($CodexAppServerPid)
        if ($process.HasExited) { Stop-Qualification 'CODEX_APP_SERVER_NOT_RUNNING' }
        $actualImage = [System.IO.Path]::GetFullPath($process.MainModule.FileName)
        if (-not [string]::Equals($actualImage, $expectedImage, [StringComparison]::OrdinalIgnoreCase)) { Stop-Qualification 'CODEX_APP_SERVER_IMAGE_MISMATCH' }
    }
    catch { Stop-Qualification 'CODEX_APP_SERVER_PROCESS_UNREADABLE' }
    $config = Read-JsonFile $AdapterConfigPath
    if (-not ($config.endpoint -is [string])) { Stop-Qualification 'CODEX_ADAPTER_ENDPOINT_MISSING' }
    try { $uri = [Uri]::new([string]$config.endpoint) }
    catch { Stop-Qualification 'CODEX_ADAPTER_ENDPOINT_INVALID' }
    if ($uri.Scheme -notin @('ws', 'wss') -or -not $uri.IsLoopback -or
        -not [string]::IsNullOrEmpty($uri.UserInfo) -or -not [string]::IsNullOrEmpty($uri.Query)) {
        Stop-Qualification 'CODEX_ADAPTER_ENDPOINT_NOT_LOCAL_ATTACH'
    }
    $process.Dispose()
    return [ordered]@{
        pid = $CodexAppServerPid
        image_sha256 = Get-FileSha256 $expectedImage
        endpoint_sha256 = [Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData([System.Text.Encoding]::UTF8.GetBytes($config.endpoint))).ToLowerInvariant()
        listener_pid_correlation = 'not_proven_without_native_connection'
        subscription_account = 'not_proven'
    }
}

function Get-RouteModelFacts {
    param([Parameter(Mandatory)] $Route, [Parameter(Mandatory)] $Contract)
    $options = $Route.native_options
    $modelId = $null
    $provider = $null
    $effort = $null
    if ($Contract.module_id -eq 'eliot.opencode.v2') {
        $model = $options.model
        $provider = [string]$model.providerID
        $modelId = [string]$model.id
        $effort = [string]$model.variant
        $qualifiedModelRef = '{0}/{1}' -f $provider, $modelId
        $modelIdMatches = [string]::Equals($modelId, [string]$Contract.expected_model_ref, [StringComparison]::Ordinal)
        $qualifiedRefMatches = [string]::Equals($qualifiedModelRef, [string]$Contract.expected_model_ref, [StringComparison]::Ordinal)
        if ([string]::IsNullOrWhiteSpace($provider) -or [string]::IsNullOrWhiteSpace($modelId) -or
            (-not $modelIdMatches -and -not $qualifiedRefMatches)) {
            Stop-Qualification 'ROUTE_MODEL_MISMATCH'
        }
        if ($null -ne $Contract.expected_effort -and $effort -ne $Contract.expected_effort) { Stop-Qualification 'ROUTE_VARIANT_MISMATCH' }
    }
    elseif ($Contract.runtime -eq 'codex') {
        $provider = [string]$options.modelProvider
        $modelId = [string]$options.model
        if ([string]::IsNullOrWhiteSpace($provider) -or [string]::IsNullOrWhiteSpace($modelId) -or
            [string]::IsNullOrWhiteSpace([string]$options.workspaceRoot) -or
            -not [System.IO.Path]::IsPathFullyQualified([string]$options.workspaceRoot)) {
            Stop-Qualification 'CODEX_ROUTE_CONFIG_INCOMPLETE'
        }
    }
    elseif ($Contract.runtime -eq 'command') {
        if ($Route.workspace_option -cne 'workspaceRoot' -or
            -not ($options -is [System.Collections.IDictionary])) {
            Stop-Qualification 'COMMAND_ROUTE_CONFIG_INCOMPLETE'
        }
        $optionKeys = @($options.Keys | ForEach-Object { [string]$_ } | Sort-Object -CaseSensitive)
        if (($optionKeys -join ',') -cne 'modelId,workspaceRoot' -or
            -not ($options.modelId -is [string]) -or
            -not ($options.workspaceRoot -is [string]) -or
            -not [System.IO.Path]::IsPathFullyQualified([string]$options.workspaceRoot)) {
            Stop-Qualification 'COMMAND_ROUTE_CONFIG_INCOMPLETE'
        }
        $modelId = [string]$options.modelId
        if ([string]::IsNullOrWhiteSpace($modelId) -or
            ($null -ne $Contract.expected_model -and $modelId -cne $Contract.expected_model)) {
            Stop-Qualification 'ROUTE_MODEL_MISMATCH'
        }
    }
    else {
        $modelId = [string]$options.modelId
        if ([string]::IsNullOrWhiteSpace($modelId) -or
            ($null -ne $Contract.expected_model -and $modelId -cne $Contract.expected_model)) {
            Stop-Qualification 'ROUTE_MODEL_MISMATCH'
        }
    }
    return [pscustomobject]@{ model_id = $modelId; provider_id = $provider; variant = $effort; requested_model_ref = $Contract.expected_model_ref }
}

function Get-ModuleCatalog {
    param([Parameter(Mandatory)][int] $TimeoutMilliseconds)
    $all = [System.Collections.Generic.List[object]]::new()
    $after = 0
    $catalogRevision = $null
    for ($page = 0; $page -lt 32; $page++) {
        $call = Invoke-ManagerCall -Method 'module.catalog.get' -Params ([ordered]@{ after = $after; limit = 8 }) -TimeoutMilliseconds $TimeoutMilliseconds
        if (-not $call.completed -or $call.exit_code -ne 0 -or $null -eq $call.value) { return [pscustomobject]@{ success = $false; code = $call.error_code; descriptors = @(); revision = $null } }
        $value = $call.value
        if ($null -eq $catalogRevision) { $catalogRevision = [long]$value.catalog_revision }
        elseif ([long]$value.catalog_revision -ne $catalogRevision) { return [pscustomobject]@{ success = $false; code = 'CATALOG_CHANGED_DURING_READ'; descriptors = @(); revision = $null } }
        foreach ($descriptor in $value.descriptors) { $all.Add($descriptor) }
        if ($null -eq $value.next_after) { return [pscustomobject]@{ success = $true; code = $null; descriptors = @($all.ToArray()); revision = $catalogRevision; selections = $value.route_selections } }
        $next = [int]$value.next_after
        if ($next -le $after) { return [pscustomobject]@{ success = $false; code = 'CATALOG_CURSOR_INVALID'; descriptors = @(); revision = $null } }
        $after = $next
    }
    return [pscustomobject]@{ success = $false; code = 'CATALOG_PAGE_LIMIT'; descriptors = @(); revision = $null }
}

function Get-Operations {
    param([Parameter(Mandatory)][int] $TimeoutMilliseconds)
    $all = [System.Collections.Generic.List[object]]::new()
    $after = 0
    for ($page = 0; $page -lt 4; $page++) {
        $call = Invoke-ManagerCall -Method 'operation.list' -Params ([ordered]@{ after = $after; limit = 50 }) -TimeoutMilliseconds $TimeoutMilliseconds
        if (-not $call.completed -or $call.exit_code -ne 0 -or $null -eq $call.value) { return [pscustomobject]@{ success = $false; code = $call.error_code; items = @() } }
        foreach ($item in $call.value.items) { $all.Add($item) }
        $next = $call.value.next_after
        if ($null -eq $next) { return [pscustomobject]@{ success = $true; code = $null; items = @($all.ToArray()) } }
        if ([int]$next -le $after) { return [pscustomobject]@{ success = $false; code = 'OPERATION_CURSOR_INVALID'; items = @() } }
        $after = [int]$next
    }
    return [pscustomobject]@{ success = $false; code = 'OPERATION_PAGE_LIMIT'; items = @() }
}

function Get-ReceiptIdentity {
    param([Parameter(Mandatory)] $Operation, [Parameter(Mandatory)][string] $OperationId,
        [Parameter(Mandatory)][string] $BindingId, [Parameter(Mandatory)][long] $Generation,
        [Parameter(Mandatory)] $Contract, [AllowNull()][string] $ExpectedBuildId)
    $outcome = $Operation.result
    $receipt = $outcome.details.module_receipt
    if ($null -eq $receipt -or $outcome.operation_id -ne $OperationId -or
        $receipt.schema_version -ne 1 -or $receipt.module_id -ne $Contract.module_id -or
        $receipt.artifact.artifact_id -ne $Contract.artifact_id -or $receipt.artifact.version -ne $Contract.version -or
        $receipt.artifact.build_id -ne $ExpectedBuildId -or
        $receipt.protocol.major -ne 1 -or $receipt.protocol.minor -ne 0 -or
        $receipt.binding_id -ne $BindingId -or $receipt.binding_generation -ne $Generation -or
        $receipt.operation_id -ne $OperationId -or [string]$receipt.input_sha256 -notmatch '\A[a-f0-9]{64}\z') {
        return $false
    }
    return $true
}

function Get-ExactAgentState {
    param([Parameter(Mandatory)][string] $BindingId, [Parameter(Mandatory)][long] $Generation,
        [Parameter(Mandatory)][int] $TimeoutMilliseconds)
    return Invoke-ManagerCall -Method 'agent.state' -Params ([ordered]@{ binding_id = $BindingId; generation = $Generation }) -TimeoutMilliseconds $TimeoutMilliseconds
}

function Confirm-CodexStillAttached {
    if ($Adapter -ne 'Codex') { return $true }
    try {
        $process = [System.Diagnostics.Process]::GetProcessById($CodexAppServerPid)
        $ok = -not $process.HasExited -and [string]::Equals([System.IO.Path]::GetFullPath($process.MainModule.FileName), $CodexAppServerImagePath, [StringComparison]::OrdinalIgnoreCase)
        $process.Dispose()
        return $ok
    }
    catch { return $false }
}

function Find-TaskAttempt {
    param([Parameter(Mandatory)][string] $TaskId, [Parameter(Mandatory)][string] $ManagerId,
        [Parameter(Mandatory)][int] $TimeoutMilliseconds)
    $taskCall = Invoke-ManagerCall -Method 'task.get' -Params ([ordered]@{ task_id = $TaskId }) -TimeoutMilliseconds $TimeoutMilliseconds
    if (-not $taskCall.completed -or $taskCall.exit_code -ne 0 -or $null -eq $taskCall.value) { return $null }
    $attemptId = [string]$taskCall.value.current_attempt_id
    if ([string]::IsNullOrWhiteSpace($attemptId)) { return $null }
    $attemptCall = Invoke-ManagerCall -Method 'attempt.get' -Params ([ordered]@{ attempt_id = $attemptId }) -TimeoutMilliseconds $TimeoutMilliseconds
    if (-not $attemptCall.completed -or $attemptCall.exit_code -ne 0 -or $null -eq $attemptCall.value) { return $null }
    if ($attemptCall.value.owner_id -ne $ManagerId -or $attemptCall.value.start_owner -ne 'controller' -or $attemptCall.value.task_id -ne $TaskId) { return $null }
    return $attemptCall.value
}

function Close-IsolatedHost {
    if ($null -eq $script:HostProcess) { return }
    try {
        if (-not $script:HostProcess.HasExited -and $script:HostInputOpen) {
            $script:HostProcess.StandardInput.Close()
            $script:HostInputOpen = $false
            if (-not $script:HostProcess.WaitForExit(10000)) { $script:HostPendingProcessId = $script:HostProcess.Id }
        }
        elseif (-not $script:HostProcess.HasExited) { $script:HostPendingProcessId = $script:HostProcess.Id }
    }
    catch {
        if ($null -ne $script:HostProcess -and -not $script:HostProcess.HasExited) { $script:HostPendingProcessId = $script:HostProcess.Id }
    }
    finally { $script:HostProcess.Dispose(); $script:HostProcess = $null }
}

$summaryPath = $null
try {
    if ($PSVersionTable.PSVersion.Major -lt 7 -or -not $IsWindows) { Stop-Qualification 'POWERSHELL_7_WINDOWS_REQUIRED' }
    if ($Adapter -eq 'Claude') {
        if (-not $EnableClaude) { Stop-Qualification 'CLAUDE_OPT_IN_REQUIRED' }
        Stop-Qualification 'CLAUDE_ADAPTER_UNAVAILABLE'
    }
    $contract = Get-AdapterContract
    $script:HostPath = Assert-ExistingFile $HostExecutable
    $script:PublicCliPath = Assert-ExistingFile $PublicCliExecutable
    $actualHostHash = Get-FileSha256 $script:HostPath
    $actualPublicCliHash = Get-FileSha256 $script:PublicCliPath
    if ($actualHostHash -cne $ExpectedHostSha256.ToLowerInvariant()) { Stop-Qualification 'HOST_BINARY_HASH_MISMATCH' }
    if ($actualPublicCliHash -cne $ExpectedPublicCliSha256.ToLowerInvariant()) { Stop-Qualification 'PUBLIC_CLI_BINARY_HASH_MISMATCH' }
    $hostBuild = Get-BuildProvenance -ManifestPath $HostBuildManifestPath -ExpectedManifestSha256 $ExpectedHostBuildManifestSha256 -BinaryPath $script:HostPath -ExpectedPackage 'eliot-swarm-controller' -ExpectedTarget 'swarm-host'
    $publicCliBuild = Get-PublicCliBuildProvenance -ManifestPath $PublicCliBuildManifestPath -ExpectedManifestSha256 $ExpectedPublicCliBuildManifestSha256 -BinaryPath $script:PublicCliPath -ExpectedBinarySha256 $actualPublicCliHash
    Assert-PublicCliHostSibling -PublicCliPath $script:PublicCliPath -HostPath $script:HostPath -PublicCliBuild $publicCliBuild -HostBuild $hostBuild
    $script:ConfigPath = Assert-ExistingFile $HostConfigPath
    $outputRoot = Assert-ExistingDirectory $OutputRoot
    New-PrivateRunDirectory -Parent $outputRoot
    $taskPath = Assert-ExistingFile $TaskSpecPath
    $settingsPath = Assert-ExistingFile $LaunchSettingsPath
    $turnPath = Assert-ExistingFile $OneTurnTextPath
    $installRoot = Assert-ExistingDirectory $ModuleInstallRoot
    $executable = Assert-ExistingFile $ModuleExecutablePath
    $descriptorPath = Assert-ExistingFile $ModuleDescriptorPath
    $ownerHelper = Assert-ExistingFile $ModuleOwnerHelperPath
    $expectedHelperHash = $ExpectedModuleOwnerHelperSha256.ToLowerInvariant()
    $taskSpec = Read-JsonFile $taskPath
    Assert-TaskFixture $taskSpec
    $launchSettings = Read-JsonFile $settingsPath
    Assert-LaunchSettings $launchSettings
    $oneTurnText = Get-Content -LiteralPath $turnPath -Raw
    if ([string]::IsNullOrWhiteSpace($oneTurnText)) { Stop-Qualification 'ONE_TURN_INPUT_EMPTY' }
    $oneTurnBytes = [System.Text.Encoding]::UTF8.GetByteCount($oneTurnText)
    if ($oneTurnBytes -gt 262144) { Stop-Qualification 'ONE_TURN_INPUT_LIMIT' }
    $codexProcessFacts = $null
    if ($Adapter -eq 'Codex') {
        if ($CodexAppServerPid -le 0 -or [string]::IsNullOrWhiteSpace($CodexAppServerImagePath)) { Stop-Qualification 'CODEX_ATTACH_PID_AND_IMAGE_REQUIRED' }
        $CodexAppServerImagePath = Assert-ExistingFile $CodexAppServerImagePath
    }
    $script:Report.safe_facts.host_binary = $hostBuild
    $script:Report.safe_facts.public_cli = $publicCliBuild
    $script:Report.safe_facts.owner_helper_sha256 = (Get-FileSha256 $ownerHelper)
    $script:Report.safe_facts.task_spec_sha256 = Get-FileSha256 $taskPath
    $script:Report.safe_facts.launch_settings_sha256 = Get-FileSha256 $settingsPath
    $script:Report.safe_facts.one_turn_input_sha256 = [Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData([System.Text.Encoding]::UTF8.GetBytes($oneTurnText))).ToLowerInvariant()
    $script:Report.safe_facts.module = [ordered]@{ module_id = $contract.module_id; artifact_id = $contract.artifact_id; version = $contract.version }
    $script:Report.safe_facts.one_turn_input_bytes = $oneTurnBytes
    Assert-ModuleSupervisorConfig -Path $script:ConfigPath -InstallRoot $installRoot -DescriptorPath $descriptorPath -OwnerHelperPath $ownerHelper -OwnerHelperSha256 $expectedHelperHash
    $module = Get-InstalledModuleFacts -InstallRoot $installRoot -ExecutablePath $executable -DescriptorPath $descriptorPath -OwnerHelperPath $ownerHelper -OwnerHelperSha256 $expectedHelperHash -Contract $contract
    $moduleBuild = Get-BuildProvenance -ManifestPath $ModuleBuildManifestPath -ExpectedManifestSha256 $ExpectedModuleBuildManifestSha256 -BinaryPath $module.executable -ExpectedPackage $contract.build_package -ExpectedTarget $contract.build_target
    if ($hostBuild.source_commit -cne $moduleBuild.source_commit -or $hostBuild.source_tree -cne $moduleBuild.source_tree -or
        $hostBuild.cargo_toml_sha256 -cne $moduleBuild.cargo_toml_sha256 -or
        $hostBuild.cargo_lock_sha256 -cne $moduleBuild.cargo_lock_sha256 -or
        $hostBuild.rust_toolchain_toml_sha256 -cne $moduleBuild.rust_toolchain_toml_sha256 -or
        $hostBuild.target_dir_sha256 -cne $moduleBuild.target_dir_sha256 -or
        $hostBuild.active_toolchain -cne $moduleBuild.active_toolchain -or
        $hostBuild.pinned_channel -cne $moduleBuild.pinned_channel -or
        $hostBuild.rustc_version_verbose -cne $moduleBuild.rustc_version_verbose -or
        $hostBuild.cargo_version_verbose -cne $moduleBuild.cargo_version_verbose) {
        Stop-Qualification 'HOST_MODULE_BUILD_PROVENANCE_DIVERGED'
    }
    $script:Report.safe_facts.module.build_id = $module.descriptor.artifact.build_id
    $script:Report.safe_facts.module.executable_sha256 = $module.executable_sha256
    $script:Report.safe_facts.module.descriptor_sha256 = $module.descriptor_sha256
    $script:Report.safe_facts.host_build = $hostBuild
    $script:Report.safe_facts.module_build = $moduleBuild
    $script:Report.safe_facts.build_set = [ordered]@{ source_commit = $hostBuild.source_commit; source_tree = $hostBuild.source_tree; lock_sha256 = $hostBuild.cargo_lock_sha256; toolchain_sha256 = $hostBuild.rust_toolchain_toml_sha256; shared_active_toolchain = $hostBuild.active_toolchain }
    Add-Stage -Name 'preflight' -Status 'passed' -Facts ([ordered]@{ host_binary_sha256 = $actualHostHash; public_cli_sha256 = $actualPublicCliHash; host_build = $hostBuild; public_cli_build = $publicCliBuild; cli_host_sibling_verified = $true; cli_host_ipc_protocol_version = $publicCliBuild.host_ipc_protocol_version; module_executable_sha256 = $module.executable_sha256; descriptor_sha256 = $module.descriptor_sha256; install_receipt_sha256 = $module.receipt_sha256; module_identity = $script:Report.safe_facts.module; module_build = $moduleBuild })

    $script:CurrentStage = 'isolated_host'
    $hostArgs = @('--config', $script:ConfigPath, '--data-dir', $script:StateDirectory, 'host', '--stop-on-stdin-eof')
    $hostStart = Invoke-SwarmProcess -Arguments $hostArgs -TimeoutMilliseconds 0 -HostProcess
    if (-not $hostStart.started) { Stop-Qualification $hostStart.error_code }
    $script:HostProcess = $hostStart.process
    $script:HostInputOpen = $true
    $script:OperatorCredentialPath = Join-Path $script:StateDirectory 'operator.json'
    $hostReady = $false
    $hostDeadline = [DateTime]::UtcNow.AddSeconds([Math]::Min(30, $TimeoutSeconds))
    while ([DateTime]::UtcNow -lt $hostDeadline) {
        if ($script:HostProcess.HasExited) { break }
        if (Test-Path -LiteralPath $script:OperatorCredentialPath) {
            $operatorStatus = Invoke-ApplicationCall -CredentialPath $script:OperatorCredentialPath -Method 'host.status' -Params ([ordered]@{}) -TimeoutMilliseconds 10000
            if ($operatorStatus.completed -and $operatorStatus.exit_code -eq 0 -and $null -ne $operatorStatus.value) { $hostReady = $true; break }
        }
        Start-Sleep -Milliseconds 500
    }
    if (-not $hostReady) { Add-Stage -Name 'isolated_host' -Status 'blocked' -Code 'HOST_NOT_READY'; Stop-Qualification 'HOST_NOT_READY' }
    Add-Stage -Name 'isolated_host' -Status 'passed' -Facts ([ordered]@{ isolated_data_root = $true; host_process_id = $script:HostProcess.Id; host_image_sha256 = $actualHostHash; public_cli_image_sha256 = $actualPublicCliHash; operator_credential_created = (Test-Path -LiteralPath $script:OperatorCredentialPath) })

    $script:CurrentStage = 'manager_admission'
    $managerId = 'native-qualification-' + [Guid]::NewGuid().ToString('N')
    $script:ManagerCredentialPath = Join-Path $script:RunDirectory 'manager.json'
    $managerArgs = @('--config', $script:ConfigPath, '--data-dir', $script:StateDirectory, '--credential', $script:OperatorCredentialPath,
        'client-create', $managerId, '--role', 'manager', '--out', $script:ManagerCredentialPath)
    $managerCreate = Invoke-SwarmProcess -Arguments $managerArgs -TimeoutMilliseconds 30000
    if (-not $managerCreate.completed -or $managerCreate.exit_code -ne 0) {
        Add-Stage -Name 'manager_admission' -Status 'unknown' -Code $(if ($managerCreate.error_code) { $managerCreate.error_code } else { 'MANAGER_REGISTRATION_UNKNOWN' })
        Stop-Qualification 'MANAGER_REGISTRATION_UNKNOWN'
    }
    if (-not (Test-Path -LiteralPath $script:ManagerCredentialPath)) { Stop-Qualification 'MANAGER_CREDENTIAL_MISSING' }
    $managerCredential = Read-JsonFile $script:ManagerCredentialPath
    if ($managerCredential.client_id -ne $managerId -or [string]::IsNullOrWhiteSpace([string]$managerCredential.token)) { Stop-Qualification 'MANAGER_CREDENTIAL_INVALID' }
    $script:Report.safe_facts.manager_client_id = $managerId
    Add-Stage -Name 'manager_admission' -Status 'passed' -Facts ([ordered]@{ role = 'manager'; client_id = $managerId; credential_created_for_private_run = $true; credential_material_in_receipt = $false; gm_handover = $false })

    $script:CurrentStage = 'trusted_descriptor'
    $catalog = $null
    $catalogDeadline = [DateTime]::UtcNow.AddSeconds([Math]::Min(30, $TimeoutSeconds))
    do {
        $catalog = Get-ModuleCatalog -TimeoutMilliseconds 30000
        if ($catalog.success -and @($catalog.descriptors | Where-Object { $_.module_id -eq $contract.module_id -and $_.artifact.artifact_id -eq $contract.artifact_id -and $_.artifact.version -eq $contract.version }).Count -eq 1) { break }
        Start-Sleep -Milliseconds 500
    } while ([DateTime]::UtcNow -lt $catalogDeadline -and -not $script:HostProcess.HasExited)
    if (-not $catalog.success) { Stop-Qualification $(if ($catalog.code) { $catalog.code } else { 'MODULE_CATALOG_UNAVAILABLE' }) }
    $descriptorEntry = @($catalog.descriptors | Where-Object { $_.module_id -ceq $contract.module_id -and $_.artifact.artifact_id -ceq $contract.artifact_id -and $_.artifact.version -ceq $contract.version })
    if ($descriptorEntry.Count -ne 1 -or $descriptorEntry[0].enabled -ne $true -or $descriptorEntry[0].protocol.minimum.major -gt 1 -or $descriptorEntry[0].protocol.maximum.major -lt 1 -or
        ($descriptorEntry[0].protocol.minimum.major -eq 1 -and $descriptorEntry[0].protocol.minimum.minor -gt 0) -or
        ($descriptorEntry[0].protocol.maximum.major -eq 1 -and $descriptorEntry[0].protocol.maximum.minor -lt 0)) {
        Stop-Qualification 'TRUSTED_DESCRIPTOR_NOT_REGISTERED_OR_INCOMPATIBLE'
    }
    $catalogCaps = @($descriptorEntry[0].capabilities | ForEach-Object { [string]$_ } | Sort-Object)
    $expectedCaps = @($contract.capabilities | Sort-Object)
    $expectedCommands = @($contract.command_schemas | Sort-Object)
    $expectedEvents = @($contract.event_schemas | Sort-Object)
    $catalogCommands = @($descriptorEntry[0].command_schemas | ForEach-Object { "{0}@{1}:{2}" -f $_.schema_id, $_.version, $_.sha256 } | Sort-Object)
    $catalogEvents = @($descriptorEntry[0].event_schemas | ForEach-Object { "{0}@{1}:{2}" -f $_.schema_id, $_.version, $_.sha256 } | Sort-Object)
    $localCommands = @($module.descriptor.command_schemas | ForEach-Object { "{0}@{1}:{2}" -f $_.schema_id, $_.version, $_.sha256 } | Sort-Object)
    $localEvents = @($module.descriptor.event_schemas | ForEach-Object { "{0}@{1}:{2}" -f $_.schema_id, $_.version, $_.sha256 } | Sort-Object)
    $configSchemaMatches = (ConvertTo-Json -InputObject $descriptorEntry[0].config_schema -Depth 16 -Compress) -ceq (ConvertTo-Json -InputObject $module.descriptor.config_schema -Depth 16 -Compress)
    $protocolMatches = (ConvertTo-Json -InputObject $descriptorEntry[0].protocol -Depth 16 -Compress) -ceq (ConvertTo-Json -InputObject $module.descriptor.protocol -Depth 16 -Compress)
    if (($catalogCaps -join ',') -cne ($expectedCaps -join ',') -or
        ($catalogCommands -join ',') -cne ($expectedCommands -join ',') -or
        ($catalogEvents -join ',') -cne ($expectedEvents -join ',') -or
        ($localCommands -join ',') -cne ($catalogCommands -join ',') -or
        ($localEvents -join ',') -cne ($catalogEvents -join ',') -or
        -not $configSchemaMatches -or -not $protocolMatches -or
        $descriptorEntry[0].artifact.build_id -ne $module.descriptor.artifact.build_id -or
        $descriptorEntry[0].lifecycle -ne $module.descriptor.lifecycle -or
        $descriptorEntry[0].activation -ne $module.descriptor.activation) {
        Stop-Qualification 'TRUSTED_DESCRIPTOR_CONTRACT_MISMATCH'
    }
    Add-Stage -Name 'trusted_descriptor' -Status 'passed' -Facts ([ordered]@{ module_id = $contract.module_id; artifact_id = $contract.artifact_id; version = $contract.version; registered_revision = $descriptorEntry[0].registered_revision; catalog_revision = $catalog.revision; protocol = '1.0'; capability_count = $catalogCaps.Count })

    $script:CurrentStage = 'route_configuration'
    $routeCall = Invoke-ManagerCall -Method 'route.list' -Params ([ordered]@{}) -TimeoutMilliseconds 30000
    if (-not $routeCall.completed -or $routeCall.exit_code -ne 0 -or $null -eq $routeCall.value) { Stop-Qualification $(if ($routeCall.error_code) { $routeCall.error_code } else { 'ROUTE_LIST_UNAVAILABLE' }) }
    $routes = @($routeCall.value.routes | Where-Object { $_.runtime -ceq $contract.runtime -and $_.module_artifact_id -ceq $contract.artifact_id -and $_.enabled -eq $true })
    if ($routes.Count -ne 1) { Stop-Qualification 'EXACT_ENABLED_ROUTE_NOT_UNIQUE' }
    $route = $routes[0]
    $routeModel = Get-RouteModelFacts -Route $route -Contract $contract
    if ($null -ne $launchSettings.requested_model -and $launchSettings.requested_model -cne $routeModel.model_id) { Stop-Qualification 'REQUESTED_MODEL_DIFFERS_FROM_ROUTE' }
    if ($null -eq $launchSettings.requested_model -and $null -ne $routeModel.model_id) { Stop-Qualification 'REQUESTED_MODEL_MUST_MATCH_ROUTE' }
    if ($null -ne $launchSettings.requested_effort -and ($null -eq $routeModel.variant -or $launchSettings.requested_effort -cne $routeModel.variant)) { Stop-Qualification 'REQUESTED_EFFORT_DIFFERS_FROM_ROUTE' }
    if ($null -ne $routeModel.variant -and $null -eq $launchSettings.requested_effort -and $contract.module_id -eq 'eliot.opencode.v2' -and $routeModel.variant -ne '') {
        # Effort remains optional; the exact configured route variant is retained only when the caller explicitly selects it.
    }
    $script:Report.safe_facts.route = [ordered]@{ alias = $route.alias; runtime = $route.runtime; artifact_id = $route.module_artifact_id; model_id = $routeModel.model_id; provider_id = $routeModel.provider_id; variant = $routeModel.variant; requested_model_ref = $routeModel.requested_model_ref }
    if ($Adapter -eq 'Codex') {
        $module.config_path = Get-AdapterConfigPath -Descriptor $module.descriptor
        $codexProcessFacts = Assert-CodexProcessAttachment -AdapterConfigPath $module.config_path
        $script:Report.safe_facts.codex_attachment = $codexProcessFacts
    }
    else {
        $module.config_path = Get-AdapterConfigPath -Descriptor $module.descriptor
        $script:Report.safe_facts.adapter_config_sha256 = Get-FileSha256 $module.config_path
    }
    if ($Adapter -eq 'Command') {
        $script:Report.safe_facts.command_config = Assert-CommandAdapterConfig -Path $module.config_path
    }
    Add-Stage -Name 'route_configuration' -Status 'passed' -Facts ([ordered]@{ route_alias = $route.alias; runtime = $route.runtime; artifact_id = $route.module_artifact_id; model_id = $routeModel.model_id; provider_id = $routeModel.provider_id; variant = $routeModel.variant; requested_model_ref = $routeModel.requested_model_ref })

    $script:CurrentStage = 'manager_route_selection'
    $selectionParams = [ordered]@{
        route_alias = [string]$route.alias
        module_id = $contract.module_id
        artifact_id = $contract.artifact_id
        version = $contract.version
        expected_catalog_revision = [long]$catalog.revision
        client_request_id = New-RequestId
    }
    $select = Invoke-ManagerCall -Method 'module.route.select' -Params $selectionParams
    if (-not $select.completed -or $select.exit_code -ne 0 -or $null -eq $select.value) {
        $readCatalog = Get-ModuleCatalog -TimeoutMilliseconds 30000
        $observedSelection = if ($readCatalog.success -and $null -ne $readCatalog.selections) { $readCatalog.selections[[string]$route.alias] } else { $null }
        if ($null -eq $observedSelection -or $observedSelection.module_id -ne $contract.module_id -or $observedSelection.artifact.artifact_id -ne $contract.artifact_id -or $observedSelection.artifact.version -ne $contract.version) {
            Add-Stage -Name 'manager_route_selection' -Status 'unknown' -Code $(if ($select.error_code) { $select.error_code } else { 'ROUTE_SELECTION_UNKNOWN' })
            Stop-Qualification 'ROUTE_SELECTION_UNKNOWN'
        }
        $selectionRevision = [long]$observedSelection.selected_revision
    }
    else { $selectionRevision = [long]$select.value.selection.selected_revision }
    Add-Stage -Name 'manager_route_selection' -Status 'passed' -Facts ([ordered]@{ route_alias = $route.alias; module_id = $contract.module_id; artifact_id = $contract.artifact_id; selected_revision = $selectionRevision; scope = 'manager_future_bindings_only' })

    $script:CurrentStage = 'task_create'
    $taskCreate = Invoke-ManagerCall -Method 'task.create' -Params ([ordered]@{ project_id = $ProjectId; spec = $taskSpec; client_request_id = New-RequestId })
    if (-not $taskCreate.completed -or $taskCreate.exit_code -ne 0 -or $null -eq $taskCreate.value -or [string]::IsNullOrWhiteSpace([string]$taskCreate.value.task_id)) {
        Add-Stage -Name 'task_create' -Status 'unknown' -Code $(if ($taskCreate.error_code) { $taskCreate.error_code } else { 'TASK_CREATE_OUTCOME_UNKNOWN' })
        Stop-Qualification 'TASK_CREATE_OUTCOME_UNKNOWN'
    }
    $taskId = [string]$taskCreate.value.task_id
    if ($taskCreate.value.revision -ne 1) { Stop-Qualification 'TASK_CREATE_REVISION_UNEXPECTED' }
    $script:Report.safe_facts.task_id = $taskId
    Add-Stage -Name 'task_create' -Status 'passed' -Facts ([ordered]@{ task_id = $taskId; revision = 1; project_id = $ProjectId; source = 'caller_supplied_task_spec' })

    $script:CurrentStage = 'task_claim'
    $claim = Invoke-ManagerCall -Method 'task.claim' -Params ([ordered]@{ task_id = $taskId; expected_revision = 1; owner_id = $managerId; start_owner = 'controller'; client_request_id = New-RequestId })
    $attempt = Find-TaskAttempt -TaskId $taskId -ManagerId $managerId -TimeoutMilliseconds 30000
    if ($null -eq $attempt) {
        Add-Stage -Name 'task_claim' -Status 'unknown' -Code $(if ($claim.error_code) { $claim.error_code } else { 'TASK_CLAIM_READBACK_FAILED' })
        Stop-Qualification 'TASK_CLAIM_READBACK_FAILED'
    }
    $attemptId = [string]$attempt.attempt_id
    Add-Stage -Name 'task_claim' -Status 'passed' -Facts ([ordered]@{ task_id = $taskId; attempt_id = $attemptId; owner_id = $managerId; start_owner = 'controller' })

    $script:CurrentStage = 'launcher_preview'
    $previewParams = [ordered]@{
        task_id = $taskId
        expected_task_revision = 1
        route = [string]$route.alias
        agent_profile = [string]$launchSettings.agent_profile
        mcp_profile = [string]$launchSettings.mcp_profile
        mcp_surface = [string]$launchSettings.mcp_surface
        workspace_policy = [string]$launchSettings.workspace_policy
        requested_model = $launchSettings.requested_model
        requested_effort = $launchSettings.requested_effort
        budget = $launchSettings.budget
        stop_conditions = @($launchSettings.stop_conditions)
        purpose = [string]$launchSettings.purpose
    }
    $preview = Invoke-ManagerCall -Method 'swarm.launch.preview' -Params $previewParams -TimeoutMilliseconds 30000
    if (-not $preview.completed -or $preview.exit_code -ne 0 -or $null -eq $preview.value) { Stop-Qualification $(if ($preview.error_code) { $preview.error_code } else { 'LAUNCH_PREVIEW_UNAVAILABLE' }) }
    $hardBlocks = @($preview.value.hard_blocks | ForEach-Object { [string]$_ })
    if ($hardBlocks.Count -gt 0 -or $preview.value.attempt_action -eq 'forbidden') {
        $safeBlocks = @($hardBlocks | Where-Object { $_ -match '\A[a-z0-9_]{1,96}\z' })
        Add-Stage -Name 'launcher_preview' -Status 'blocked' -Code 'LAUNCH_PREVIEW_BLOCKED' -Facts ([ordered]@{ hard_blocks = $safeBlocks; attempt_action = [string]$preview.value.attempt_action })
        Stop-Qualification 'LAUNCH_PREVIEW_BLOCKED'
    }
    $planDigest = [string]$preview.value.plan_digest
    if ($planDigest -notmatch '\Asha256:[a-f0-9]{64}\z') { Stop-Qualification 'LAUNCH_PREVIEW_DIGEST_INVALID' }
    Add-Stage -Name 'launcher_preview' -Status 'passed' -Facts ([ordered]@{ plan_digest = $planDigest; hard_blocks = @(); attempt_id = $attemptId; workspace_policy = $launchSettings.workspace_policy })

    $script:CurrentStage = 'launcher_launch'
    $launchParams = [ordered]@{ client_request_id = New-RequestId; plan_digest = $planDigest }
    foreach ($key in $previewParams.Keys) { $launchParams[$key] = $previewParams[$key] }
    $launch = Invoke-ManagerCall -Method 'swarm.launch' -Params $launchParams -TimeoutMilliseconds 30000
    $parentOperationId = if ($launch.completed -and $launch.exit_code -eq 0 -and $null -ne $launch.value) { [string]$launch.value.operation_id } else { $null }
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $readyProof = $null
    $launchReadbackFailure = $null
    while ([DateTime]::UtcNow -lt $deadline) {
        $operations = Get-Operations -TimeoutMilliseconds 30000
        if (-not $operations.success) { $launchReadbackFailure = $operations.code; Start-Sleep -Milliseconds 750; continue }
        if ([string]::IsNullOrWhiteSpace($parentOperationId)) {
            $parents = @($operations.items | Where-Object { $_.method -eq 'swarm.launch' -and $_.task_id -eq $taskId -and $_.attempt_id -eq $attemptId })
            if ($parents.Count -eq 1) { $parentOperationId = [string]$parents[0].operation_id }
        }
        if ([string]::IsNullOrWhiteSpace($parentOperationId)) { Start-Sleep -Milliseconds 750; continue }
        $openOps = @($operations.items | Where-Object { $_.method -eq 'agent.open' -and $_.prerequisite_operation_id -eq $parentOperationId -and $_.task_id -eq $taskId -and $_.attempt_id -eq $attemptId })
        if ($openOps.Count -ne 1) { Start-Sleep -Milliseconds 750; continue }
        $parentCall = Invoke-ManagerCall -Method 'operation.get' -Params ([ordered]@{ operation_id = $parentOperationId }) -TimeoutMilliseconds 30000
        $openCall = Invoke-ManagerCall -Method 'operation.get' -Params ([ordered]@{ operation_id = [string]$openOps[0].operation_id }) -TimeoutMilliseconds 30000
        if (-not $parentCall.completed -or $parentCall.exit_code -ne 0 -or -not $openCall.completed -or $openCall.exit_code -ne 0) { Start-Sleep -Milliseconds 750; continue }
        $parentOp = $parentCall.value
        $openOp = $openCall.value
        $bindingId = [string]$openOp.binding_id
        $generation = [long]$openOp.binding_generation
        if ([string]::IsNullOrWhiteSpace($bindingId) -or $generation -le 0 -or
            $openOp.state -ne 'settled' -or $openOp.result.outcome -ne 'applied' -or
            $openOp.prerequisite_operation_id -ne $parentOperationId -or
            $openOp.task_id -ne $taskId -or $openOp.attempt_id -ne $attemptId -or
            $parentOp.method -ne 'swarm.launch' -or $parentOp.task_id -ne $taskId -or $parentOp.attempt_id -ne $attemptId) {
            Start-Sleep -Milliseconds 750; continue
        }
        if (-not (Get-ReceiptIdentity -Operation $openOp -OperationId ([string]$openOp.operation_id) -BindingId $bindingId -Generation $generation -Contract $contract -ExpectedBuildId $module.descriptor.artifact.build_id)) {
            $launchReadbackFailure = 'OPEN_MODULE_RECEIPT_MISMATCH'
            Start-Sleep -Milliseconds 750; continue
        }
        $agentState = Get-ExactAgentState -BindingId $bindingId -Generation $generation -TimeoutMilliseconds 30000
        if (-not $agentState.completed -or $agentState.exit_code -ne 0 -or $null -eq $agentState.value) { Start-Sleep -Milliseconds 750; continue }
        $state = $agentState.value
        $supervisor = $state.observation.module_supervisor
        if ($state.binding_id -ne $bindingId -or $state.generation -ne $generation -or
            $state.state -ne 'ready' -or $supervisor.phase -ne 'ready' -or
            $supervisor.module_id -ne $contract.module_id -or $supervisor.artifact_id -ne $contract.artifact_id -or
            $supervisor.artifact_version -ne $contract.version -or
            $supervisor.binding_id -ne $bindingId -or $supervisor.generation -ne $generation) {
            Start-Sleep -Milliseconds 750; continue
        }
        $readyProof = [pscustomobject]@{
            parent = $parentOp; open = $openOp; binding_id = $bindingId; generation = $generation
            state = $state; supervisor = $supervisor
        }
        break
    }
    if ($null -eq $readyProof) {
        $code = if ($launch.error_code) { $launch.error_code } elseif ($launchReadbackFailure) { $launchReadbackFailure } else { 'LAUNCH_READY_READBACK_TIMEOUT' }
        Add-Stage -Name 'module_open_and_hello' -Status 'unknown' -Code $code -Facts ([ordered]@{ parent_operation_id = $parentOperationId; dispatch_started = $false })
        Stop-Qualification $code
    }
    $parentOperationId = [string]$readyProof.parent.operation_id
    $openOperationId = [string]$readyProof.open.operation_id
    $bindingId = [string]$readyProof.binding_id
    $generation = [long]$readyProof.generation
    $script:Report.safe_facts.launch = [ordered]@{ parent_operation_id = $parentOperationId; parent_state = $readyProof.parent.state; open_operation_id = $openOperationId; open_state = $readyProof.open.state; binding_id = $bindingId; generation = $generation; supervisor_phase = 'ready'; module_id = $contract.module_id; artifact_id = $contract.artifact_id; version = $contract.version; handshake = 'store_accepted_ready_observation'; typed_receipt = 'store_readback_identity_matches' }
    Add-Stage -Name 'module_open_and_hello' -Status 'passed' -Facts $script:Report.safe_facts.launch

    $script:CurrentStage = 'single_task_dispatch'
    if (-not (Confirm-CodexStillAttached)) { Stop-Qualification 'CODEX_APP_SERVER_CHANGED_BEFORE_DISPATCH' }
    $dispatchParams = [ordered]@{ attempt_id = $attemptId; launch_operation_id = $parentOperationId; text = $oneTurnText; client_request_id = New-RequestId }
    $script:DispatchSubmitted = $true
    $script:Report.limits.task_dispatch_calls = 1
    $dispatch = Invoke-ManagerCall -Method 'task.dispatch' -Params $dispatchParams -TimeoutMilliseconds 30000
    $dispatchOperationId = if ($dispatch.completed -and $dispatch.exit_code -eq 0 -and $null -ne $dispatch.value) { [string]$dispatch.value.operation_id } else { $null }
    $dispatchDeadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $dispatchOperation = $null
    $dispatchReadbackCode = $null
    $terminalDispatchStates = @('settled', 'rejected', 'cancelled', 'outcome_unknown')
    while ([DateTime]::UtcNow -lt $dispatchDeadline) {
        if (-not [string]::IsNullOrWhiteSpace($dispatchOperationId)) {
            $operationCall = Invoke-ManagerCall -Method 'operation.get' -Params ([ordered]@{ operation_id = $dispatchOperationId }) -TimeoutMilliseconds 5000
            if ($operationCall.completed -and $operationCall.exit_code -eq 0 -and $null -ne $operationCall.value) {
                $candidate = $operationCall.value
                if ($candidate.operation_id -ne $dispatchOperationId -or $candidate.method -ne 'task.dispatch' -or
                    $candidate.task_id -ne $taskId -or $candidate.attempt_id -ne $attemptId) {
                    $dispatchReadbackCode = 'DISPATCH_OPERATION_IDENTITY_MISMATCH'
                }
                else {
                    $dispatchOperation = $candidate
                    if ($candidate.state -in $terminalDispatchStates) { break }
                    if ($candidate.state -notin @('queued', 'sending', 'native_accepted')) { $dispatchReadbackCode = 'DISPATCH_OPERATION_STATE_UNKNOWN' }
                }
            }
            else { $dispatchReadbackCode = $operationCall.error_code }
        }
        if ([string]::IsNullOrWhiteSpace($dispatchOperationId) -or $null -eq $dispatchOperation) {
            $operations = Get-Operations -TimeoutMilliseconds 5000
            if ($operations.success) {
                $matches = @($operations.items | Where-Object { $_.method -eq 'task.dispatch' -and $_.task_id -eq $taskId -and $_.attempt_id -eq $attemptId })
                if ($matches.Count -eq 1) {
                    $dispatchOperationId = [string]$matches[0].operation_id
                }
                elseif ($matches.Count -gt 1) { $dispatchReadbackCode = 'MULTIPLE_DISPATCH_OPERATIONS' }
            }
            else { $dispatchReadbackCode = $operations.code }
        }
        Start-Sleep -Milliseconds 750
    }
    $codexStillAttached = Confirm-CodexStillAttached
    if ($null -eq $dispatchOperation) {
        $script:Report.status = 'dispatch_outcome_unknown'
        $script:Report.qualification = 'not_qualified'
        $dispatchCode = if ($dispatch.error_code) { $dispatch.error_code } elseif ($dispatchReadbackCode) { $dispatchReadbackCode } else { 'DISPATCH_READBACK_TIMEOUT' }
        Add-Stage -Name 'single_task_dispatch' -Status 'unknown' -Code $dispatchCode -Facts ([ordered]@{ dispatch_operation_id = $dispatchOperationId; readback_completed = $false; replayed = $false; codex_process_still_matches = $codexStillAttached })
    }
    else {
        $rawOperationState = [string]$dispatchOperation.state
        $knownDispatchStates = @('queued', 'sending', 'native_accepted', 'outcome_unknown', 'settled', 'rejected', 'cancelled')
        $operationState = if ($rawOperationState -in $knownDispatchStates) { $rawOperationState } else { 'unknown' }
        $stateCode = if ($operationState -eq 'unknown') { 'DISPATCH_OPERATION_STATE_UNKNOWN' } else { $null }
        $operationResult = $dispatchOperation.result
        $operationDetails = Get-OptionalField $operationResult 'details'
        $rawOutcomeValue = Get-OptionalField $operationResult 'outcome'
        $rawOutcome = if ($null -ne $rawOutcomeValue) { [string]$rawOutcomeValue } else { $null }
        $outcome = if ($rawOutcome -in @('accepted', 'applied', 'rejected', 'unknown')) { $rawOutcome } else { $null }
        $diagnostic = $null
        if ($null -ne $operationResult) {
            $operationError = Get-OptionalField $operationResult 'error'
            $errorData = Get-OptionalField $operationError 'data'
            foreach ($candidateCode in @(
                [string](Get-OptionalField $operationDetails 'diagnostic_code'),
                [string](Get-OptionalField $errorData 'code'),
                [string](Get-OptionalField $operationError 'code')
            )) {
                if ($candidateCode -match '\A[A-Z0-9_]{1,64}\z') { $diagnostic = $candidateCode; break }
            }
        }
        if ($diagnostic -notmatch '\A[A-Z0-9_]{1,64}\z') { $diagnostic = $null }
        $receiptVerified = $false
        $receiptCode = $null
        $moduleReceipt = Get-OptionalField $operationDetails 'module_receipt'
        if ($null -ne $moduleReceipt) {
            if (Get-ReceiptIdentity -Operation $dispatchOperation -OperationId $dispatchOperationId -BindingId $bindingId -Generation $generation -Contract $contract -ExpectedBuildId $module.descriptor.artifact.build_id) {
                $receiptVerified = $true
            }
            else { $receiptCode = 'DISPATCH_MODULE_RECEIPT_MISMATCH' }
        }
        elseif ($outcome -eq 'applied') { $receiptCode = 'DISPATCH_MODULE_RECEIPT_MISSING' }
        $isReceiptFailure = $null -ne $receiptCode
        $isReadbackInconsistent = $isReceiptFailure -or $null -ne $stateCode
        $script:Report.status = if ($isReadbackInconsistent) { 'dispatch_readback_inconsistent' } elseif ($operationState -in $terminalDispatchStates) { 'dispatch_readback_complete' } else { 'dispatch_pending' }
        $script:Report.qualification = 'not_qualified'
        $safeDiagnostic = if ($receiptCode) { $receiptCode } elseif ($stateCode) { $stateCode } else { $diagnostic }
        $dispatchFacts = [ordered]@{ task_id = $taskId; attempt_id = $attemptId; operation_id = [string]$dispatchOperation.operation_id; operation_state = $operationState; outcome = $outcome; diagnostic_code = $safeDiagnostic; module_receipt_verified = $receiptVerified; replayed = $false; codex_process_still_matches = $codexStillAttached }
        $stageStatus = if ($isReadbackInconsistent -or $operationState -eq 'outcome_unknown') { 'unknown' } elseif ($operationState -in @('settled', 'rejected', 'cancelled')) { 'observed' } else { 'pending' }
        Add-Stage -Name 'single_task_dispatch' -Status $stageStatus -Code $safeDiagnostic -Facts $dispatchFacts
        $script:Report.safe_facts.dispatch = $dispatchFacts
    }

    if (-not $codexStillAttached -and $Adapter -eq 'Codex') {
        $script:Report.limits.unsupported = @($script:Report.limits.unsupported) + 'codex_app_server_identity_changed_during_run'
    }
}
catch {
    if ($null -eq $script:FailureCode) { $script:FailureCode = 'HARNESS_FAILED' }
    if ($script:FailureCode -eq 'CLAUDE_ADAPTER_UNAVAILABLE') {
        $script:Report.limits.unsupported = @($script:Report.limits.unsupported) + 'no_claude_adapter_artifact_or_route_contract'
    }
    if ($script:Report.status -eq 'running') { $script:Report.status = 'blocked' }
    if ($script:Stages.Count -eq 0 -or $script:Stages[$script:Stages.Count - 1].status -notin @('blocked', 'unknown')) {
        Add-Stage -Name $script:CurrentStage -Status 'blocked' -Code $script:FailureCode
    }
}
finally {
    Close-IsolatedHost
    $activeCliProcessPending = $false
    foreach ($pendingRequest in $script:TimedOutRequestFiles) {
        $stillRunning = $false
        try {
            $timedOutProcess = [System.Diagnostics.Process]::GetProcessById([int]$pendingRequest.public_cli_process_id)
            $stillRunning = -not $timedOutProcess.HasExited
            $timedOutProcess.Dispose()
        }
        catch { $stillRunning = $false }
        if ($stillRunning) { $activeCliProcessPending = $true }
        if (-not $stillRunning) { try { [System.IO.File]::Delete([string]$pendingRequest.path) } catch { } }
    }
    foreach ($pendingProcessId in $script:PublicCliProcessIdsPending) {
        try {
            $timedOutProcess = [System.Diagnostics.Process]::GetProcessById([int]$pendingProcessId)
            if (-not $timedOutProcess.HasExited) { $activeCliProcessPending = $true }
            $timedOutProcess.Dispose()
        }
        catch { }
    }
    if ($null -eq $script:HostPendingProcessId -and -not $activeCliProcessPending) {
        foreach ($credentialPath in @($script:ManagerCredentialPath, $script:OperatorCredentialPath)) {
            if (-not [string]::IsNullOrWhiteSpace([string]$credentialPath) -and (Test-Path -LiteralPath $credentialPath -PathType Leaf)) {
                try { [System.IO.File]::Delete([string]$credentialPath) } catch { }
            }
        }
    }
    if ($null -ne $script:HostPendingProcessId) {
        $script:Report.safe_facts.host_shutdown = [ordered]@{ graceful_eof_sent = $true; pending_host_process_id = $script:HostPendingProcessId; force_terminated = $false }
        $script:Report.status = if ($script:Report.status -eq 'running') { 'host_shutdown_pending' } else { $script:Report.status }
    }
    elseif ($null -ne $script:HostProcess -or $null -ne $script:OperatorCredentialPath) {
        $script:Report.safe_facts.host_shutdown = [ordered]@{ graceful_eof_sent = $true; exited = $true; force_terminated = $false }
    }
    if ($script:PublicCliProcessIdsPending.Count -gt 0) { $script:Report.safe_facts.public_cli_processes_timed_out = @($script:PublicCliProcessIdsPending.ToArray()) }
    if ($script:Report.status -eq 'running') { $script:Report.status = if ($script:FailureCode) { 'blocked' } else { 'dispatch_readback_complete' } }
    $script:Report.completed_at_utc = [DateTime]::UtcNow.ToString('o')
    Save-Report
    if (-not [string]::IsNullOrWhiteSpace($script:RunDirectory)) { $summaryPath = Join-Path $script:RunDirectory 'qualification-receipt.json' }
}

if ($summaryPath) {
    [pscustomobject]@{
        status = $script:Report.status
        qualification = $script:Report.qualification
        adapter = $Adapter
        run_id = $script:Report.run_id
        receipt_path = $summaryPath
        dispatch_calls = $script:Report.limits.task_dispatch_calls
        failure_code = $script:FailureCode
    }
}
else {
    [pscustomobject]@{ status = 'blocked'; adapter = $Adapter; failure_code = $(if ($script:FailureCode) { $script:FailureCode } else { 'OUTPUT_ROOT_INVALID' }) }
}
