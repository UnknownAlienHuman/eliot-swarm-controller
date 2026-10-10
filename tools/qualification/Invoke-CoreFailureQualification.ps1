#requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $HostExecutable,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedHostSha256,
    [Parameter(Mandatory)][string] $HostBuildManifestPath,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedHostBuildManifestSha256,
    [Parameter(Mandatory)][string] $HostSupervisorExecutable,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedHostSupervisorSha256,
    [Parameter(Mandatory)][string] $HostSupervisorBuildManifestPath,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedHostSupervisorBuildManifestSha256,
    [Parameter(Mandatory)][string] $HostLauncherExecutable,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedHostLauncherSha256,
    [Parameter(Mandatory)][string] $HostLauncherBuildManifestPath,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedHostLauncherBuildManifestSha256,
    [Parameter(Mandatory)][string] $PublicCliExecutable,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedPublicCliSha256,
    [Parameter(Mandatory)][string] $PublicCliBuildManifestPath,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedPublicCliBuildManifestSha256,
    [Parameter(Mandatory)][string] $HostConfigPath,
    [Parameter(Mandatory)][string] $OutputRoot,
    [Parameter(Mandatory)][ValidatePattern('^[A-Za-z0-9._:-]{1,128}$')][string] $ProjectId,
    [Parameter(Mandatory)][string] $TaskSpecPath,
    [ValidatePattern('^[A-Za-z0-9._:-]{1,128}$')][string] $HookProjectId,
    [ValidatePattern('^[A-Fa-f0-9]{40}([A-Fa-f0-9]{24})?$')][string] $HookCommitOid,
    [ValidateRange(15, 180)][int] $TimeoutSeconds = 45
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:ContractBaseCommit = '410c327dd6172856bd8808da5938b2be9e1e4f9a'
$script:MaxCliOutputCharacters = 1MB
$script:MaxFrameBytes = 1MB
$script:RunDirectory = $null
$script:HostPath = $null
$script:HostSupervisorPath = $null
$script:HostLauncherPath = $null
$script:PublicCliPath = $null
$script:HostBuild = $null
$script:HostSupervisorBuild = $null
$script:HostLauncherBuild = $null
$script:PublicCliBuild = $null
$script:ConfigPath = $null
$script:PendingPublicCliPids = [System.Collections.Generic.List[int]]::new()
$script:PendingHostPids = [System.Collections.Generic.List[int]]::new()
$script:Scenarios = [System.Collections.Generic.List[object]]::new()
$script:Summary = [ordered]@{
    schema_version = 1
    harness = 'eliot.core_failure_qualification.v1'
    source_contract_base_commit = $script:ContractBaseCommit
    status = 'not_started'
    qualification = 'not_qualified'
    started_at_utc = [DateTime]::UtcNow.ToString('o')
    host = $null
    scenarios = @()
    limits = [ordered]@{
        native_calls = 0
        model_calls = 0
        mutation_replay = $false
        uncertain_native_effect_replay = $false
        current_codex_or_opencode_process_control = 'none'
        process_control = 'only this harness own-host stdin EOF; no forced termination'
        raw_contents_in_receipt = $false
        task_spec_retained_in_private_store = $true
        no_public_optional_worker_fault_trigger = $true
        source_effect_unknown_state = 'only claimed when exact Manager/HookSource readback proves the corresponding durable record'
    }
}

function Stop-Harness {
    param([Parameter(Mandatory)][string] $Code)
    throw [System.InvalidOperationException]::new($Code)
}

function Assert-SafeAbsolutePath {
    param([Parameter(Mandatory)][string] $Path, [switch] $MustExist, [switch] $Directory)
    if (-not [System.IO.Path]::IsPathFullyQualified($Path)) { Stop-Harness 'PATH_MUST_BE_ABSOLUTE' }
    $full = [System.IO.Path]::GetFullPath($Path)
    $root = [System.IO.Path]::GetPathRoot($full)
    $current = $root
    foreach ($part in ($full.Substring($root.Length) -split '[\\/]')) {
        if ([string]::IsNullOrEmpty($part)) { continue }
        $current = Join-Path $current $part
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force
            if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) { Stop-Harness 'REPARSE_PATH_NOT_ALLOWED' }
        }
    }
    if ($MustExist) {
        $item = Get-Item -LiteralPath $full -Force -ErrorAction SilentlyContinue
        if ($null -eq $item) { Stop-Harness 'INPUT_PATH_NOT_FOUND' }
        if ($Directory -and -not $item.PSIsContainer) { Stop-Harness 'INPUT_DIRECTORY_REQUIRED' }
        if (-not $Directory -and $item.PSIsContainer) { Stop-Harness 'INPUT_FILE_REQUIRED' }
    }
    return $full
}

function Get-Sha256 {
    param([Parameter(Mandatory)][string] $Path)
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Get-PinnedBuildProvenance {
    param(
        [Parameter(Mandatory)][string] $ManifestPath,
        [Parameter(Mandatory)][string] $ExpectedManifestSha256,
        [Parameter(Mandatory)][string] $BinaryPath,
        [Parameter(Mandatory)][string] $ExpectedBinarySha256,
        [Parameter(Mandatory)][string] $ExpectedPackage,
        [Parameter(Mandatory)][string] $ExpectedTarget,
        [switch] $FrontendCli
    )
    $manifestFile = Assert-SafeAbsolutePath -Path $ManifestPath -MustExist
    $binary = Assert-SafeAbsolutePath -Path $BinaryPath -MustExist
    if ([System.IO.Path]::GetFileName($manifestFile) -cne 'build-manifest.json' -or
        (Get-Sha256 $manifestFile) -cne $ExpectedManifestSha256.ToLowerInvariant()) {
        Stop-Harness 'BUILD_MANIFEST_PIN_MISMATCH'
    }
    $manifest = Read-JsonFile $manifestFile
    $binaryHash = Get-Sha256 $binary
    if ($binaryHash -cne $ExpectedBinarySha256.ToLowerInvariant()) { Stop-Harness 'BINARY_IMAGE_HASH_MISMATCH' }
    if ($manifest.schema_version -ne 1 -or
        $manifest.source.checkout_clean_before -ne $true -or $manifest.source.checkout_clean_after -ne $true -or
        $manifest.source.commit -notmatch '\A[a-f0-9]{40}\z' -or $manifest.source.tree -notmatch '\A[a-f0-9]{40}\z' -or
        $manifest.source.cargo_toml_sha256 -notmatch '\A[a-f0-9]{64}\z' -or
        $manifest.source.cargo_lock_sha256 -notmatch '\A[a-f0-9]{64}\z' -or
        $manifest.source.rust_toolchain_toml_sha256 -notmatch '\A[a-f0-9]{64}\z' -or
        $manifest.build.profile -cne 'release' -or $manifest.build.package_name -cne $ExpectedPackage -or
        [string]::IsNullOrWhiteSpace([string]$manifest.toolchain.pinned_channel) -or
        [string]::IsNullOrWhiteSpace([string]$manifest.toolchain.active_toolchain) -or
        [string]::IsNullOrWhiteSpace([string]$manifest.toolchain.rustc_version_verbose) -or
        [string]::IsNullOrWhiteSpace([string]$manifest.toolchain.cargo_version_verbose)) {
        Stop-Harness 'BUILD_MANIFEST_CONTRACT_INVALID'
    }
    $hostTripleMatches = [System.Text.RegularExpressions.Regex]::Matches([string]$manifest.toolchain.rustc_version_verbose, '(?m)^host:\s*(?<triple>[A-Za-z0-9_-]+)\s*$')
    if ($hostTripleMatches.Count -ne 1) { Stop-Harness 'BUILD_MANIFEST_TARGET_TRIPLE_INVALID' }
    $arguments = @($manifest.build.cargo_arguments | ForEach-Object { [string]$_ })
    $installation = $manifest.installation
    $supervisor = $null
    $artifacts = @($manifest.artifacts)
    if ($FrontendCli) {
        if ($manifest.format -cne 'eliot.frontend_build_manifest.v1' -or
            $manifest.build.role -cne 'cli_client' -or $manifest.build.package_manifest -cne 'crates/swarm-cli/Cargo.toml' -or
            $manifest.build.binary_target -cne $ExpectedTarget -or
            [string]::IsNullOrWhiteSpace([string]$manifest.build.external_target_dir) -or
            -not [System.IO.Path]::IsPathFullyQualified([string]$manifest.build.external_target_dir)) {
            Stop-Harness 'PUBLIC_CLI_MANIFEST_CONTRACT_INVALID'
        }
        foreach ($requiredArgument in @('--release', '--locked')) {
            if ($requiredArgument -notin $arguments) { Stop-Harness 'PUBLIC_CLI_MANIFEST_ARGUMENTS_MISMATCH' }
        }
        $argumentPairs = [ordered]@{
            '--package' = $ExpectedPackage
            '--bin' = $ExpectedTarget
            '--target-dir' = [string]$manifest.build.external_target_dir
        }
        $compatibility = $manifest.compatibility
        $launcher = $compatibility.host_launcher
        $runtime = $compatibility.host_runtime
        $supervisor = $compatibility.host_supervisor
        $protocolVersion = $compatibility.host_ipc.protocol_version
        $targetTriple = [string]$compatibility.target.rustc_host_triple
        if ($null -eq $launcher -or $launcher.package_name -cne 'eliot-swarm-controller' -or
            $launcher.binary_target -cne 'swarm-host' -or @($launcher.required_arguments).Count -ne 0 -or
            $null -eq $runtime -or $runtime.package_name -cne 'swarm-kernel-host' -or
            $runtime.binary_target -cne 'swarm-kernel-host' -or
            $null -eq $supervisor -or $supervisor.package_name -cne 'swarm-supervisor' -or
            $supervisor.binary_target -cne 'swarm-supervisor' -or
            $supervisor.role -cne 'host_supervisor' -or
            $null -eq $protocolVersion -or [int]$protocolVersion -le 0 -or
            [string]::IsNullOrWhiteSpace($targetTriple) -or $targetTriple -cne $hostTripleMatches[0].Groups['triple'].Value) {
            Stop-Harness 'PUBLIC_CLI_HOST_LAUNCHER_CONTRACT_INVALID'
        }
        $targetDir = [string]$manifest.build.external_target_dir
        $installationKeys = @('installed', 'registered', 'configured', 'activated')
        $format = 'eliot.frontend_build_manifest.v1'
    }
    else {
        if ($manifest.format -cne 'eliot.module_build_manifest.v1' -or
            @($manifest.build.binary_targets).Count -ne 1 -or $manifest.build.binary_targets[0] -cne $ExpectedTarget -or
            [string]::IsNullOrWhiteSpace([string]$manifest.build.target_dir) -or
            -not [System.IO.Path]::IsPathFullyQualified([string]$manifest.build.target_dir)) {
            Stop-Harness 'HOST_MANIFEST_CONTRACT_INVALID'
        }
        foreach ($requiredArgument in @('--locked')) {
            if ($requiredArgument -notin $arguments) { Stop-Harness 'HOST_MANIFEST_ARGUMENTS_MISMATCH' }
        }
        $argumentPairs = [ordered]@{
            '--package' = $ExpectedPackage
            '--bin' = $ExpectedTarget
            '--profile' = 'release'
            '--target-dir' = [string]$manifest.build.target_dir
        }
        $targetDir = [string]$manifest.build.target_dir
        $installationKeys = @('descriptor_generated', 'installed', 'registered', 'route_enabled', 'activated')
        $format = 'eliot.module_build_manifest.v1'
        $compatibility = $manifest.compatibility
        $runtime = $compatibility.host_runtime
        $supervisor = $compatibility.host_supervisor
        $protocolVersion = $compatibility.host_ipc.protocol_version
        if ($null -eq $runtime -or $runtime.package_name -cne 'swarm-kernel-host' -or
            $runtime.binary_target -cne 'swarm-kernel-host' -or
            $null -eq $supervisor -or $supervisor.package_name -cne 'swarm-supervisor' -or
            $supervisor.binary_target -cne 'swarm-supervisor' -or
            $supervisor.role -cne 'host_supervisor' -or
            $null -eq $protocolVersion -or [int]$protocolVersion -le 0) {
            Stop-Harness 'HOST_RUNTIME_COORDINATE_INVALID'
        }
        $launcher = $null
    }
    foreach ($option in $argumentPairs.Keys) {
        $optionIndex = [Array]::IndexOf($arguments, [string]$option)
        if ($optionIndex -lt 0 -or $optionIndex + 1 -ge $arguments.Count -or $arguments[$optionIndex + 1] -cne [string]$argumentPairs[$option]) {
            Stop-Harness 'BUILD_MANIFEST_ARGUMENTS_MISMATCH'
        }
    }
    if ($manifest.schema_version -ne 1 -or $manifest.format -cne $format -or
        $artifacts.Count -ne 1 -or $artifacts[0].target_name -cne $ExpectedTarget -or
        $artifacts[0].file -cne ('bin/' + $ExpectedTarget + '.exe') -or
        [long]$artifacts[0].bytes -ne (Get-Item -LiteralPath $binary).Length -or
        $artifacts[0].artifact_sha256 -cne $binaryHash -or $artifacts[0].source_sha256 -cne $binaryHash) {
        Stop-Harness 'BUILD_ARTIFACT_IDENTITY_MISMATCH'
    }
    $builtBinary = Assert-SafeAbsolutePath -Path (Join-Path (Split-Path -LiteralPath $manifestFile) ('bin\' + $ExpectedTarget + '.exe')) -MustExist
    if ((Get-Sha256 $builtBinary) -cne $binaryHash -or
        (Get-Item -LiteralPath $builtBinary).Length -ne (Get-Item -LiteralPath $binary).Length) {
        Stop-Harness 'BUILD_IMAGE_IS_NOT_BUILD_OUTPUT'
    }
    foreach ($name in $installationKeys) {
        if ($installation[$name] -ne $false) { Stop-Harness 'BUILD_MANIFEST_HAS_INSTALL_EFFECTS' }
    }
    $targetCanonical = [System.IO.Path]::GetFullPath($targetDir).ToUpperInvariant()
    $targetIdentity = [Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData([System.Text.Encoding]::UTF8.GetBytes($targetCanonical))).ToLowerInvariant()
    return [ordered]@{
        manifest_sha256 = Get-Sha256 $manifestFile
        format = $format
        package_name = $ExpectedPackage
        binary_target = $ExpectedTarget
        binary_sha256 = $binaryHash
        binary_bytes = [long]$artifacts[0].bytes
        source_commit = $manifest.source.commit
        source_tree = $manifest.source.tree
        cargo_toml_sha256 = $manifest.source.cargo_toml_sha256
        cargo_lock_sha256 = $manifest.source.cargo_lock_sha256
        rust_toolchain_toml_sha256 = $manifest.source.rust_toolchain_toml_sha256
        target_dir_sha256 = $targetIdentity
        active_toolchain = $manifest.toolchain.active_toolchain
        pinned_channel = $manifest.toolchain.pinned_channel
        rustc_version_verbose = $manifest.toolchain.rustc_version_verbose
        rustc_host_triple = $hostTripleMatches[0].Groups['triple'].Value
        cargo_version_verbose = $manifest.toolchain.cargo_version_verbose
        host_ipc_protocol_version = $protocolVersion
        host_runtime_package_name = if ($null -ne $runtime) { $runtime.package_name } else { $null }
        host_runtime_binary_target = if ($null -ne $runtime) { $runtime.binary_target } else { $null }
        host_supervisor_package_name = if ($null -ne $supervisor) { $supervisor.package_name } else { $null }
        host_supervisor_binary_target = if ($null -ne $supervisor) { $supervisor.binary_target } else { $null }
        host_launcher_package_name = if ($null -ne $launcher) { $launcher.package_name } else { $null }
        host_launcher_binary_target = if ($null -ne $launcher) { $launcher.binary_target } else { $null }
        manifest_is_unsigned = $true
    }
}

function Assert-PublicCliHostSibling {
    param(
        [Parameter(Mandatory)][System.Collections.IDictionary] $PublicCliBuild,
        [Parameter(Mandatory)][System.Collections.IDictionary] $HostLauncherBuild,
        [Parameter(Mandatory)][System.Collections.IDictionary] $HostBuild,
        [Parameter(Mandatory)][System.Collections.IDictionary] $HostSupervisorBuild
    )
    $expectedLauncher = [System.IO.Path]::GetFullPath((Join-Path (Split-Path -LiteralPath $script:PublicCliPath) 'swarm-host.exe'))
    $expectedRuntime = [System.IO.Path]::GetFullPath((Join-Path (Split-Path -LiteralPath $script:PublicCliPath) 'swarm-kernel-host.exe'))
    $expectedSupervisor = [System.IO.Path]::GetFullPath((Join-Path (Split-Path -LiteralPath $script:PublicCliPath) 'swarm-supervisor.exe'))
    if ([System.IO.Path]::GetFileName($script:PublicCliPath) -cne 'swarm.exe' -or
        -not [string]::Equals($expectedLauncher, $script:HostLauncherPath, [StringComparison]::OrdinalIgnoreCase) -or
        -not [string]::Equals($expectedRuntime, $script:HostPath, [StringComparison]::OrdinalIgnoreCase) -or
        -not [string]::Equals($expectedSupervisor, $script:HostSupervisorPath, [StringComparison]::OrdinalIgnoreCase) -or
        $PublicCliBuild.host_launcher_package_name -cne $HostLauncherBuild.package_name -or
        $PublicCliBuild.host_launcher_binary_target -cne $HostLauncherBuild.binary_target -or
        $PublicCliBuild.host_runtime_package_name -cne $HostBuild.package_name -or
        $PublicCliBuild.host_runtime_binary_target -cne $HostBuild.binary_target -or
        $PublicCliBuild.host_supervisor_package_name -cne $HostSupervisorBuild.package_name -or
        $PublicCliBuild.host_supervisor_binary_target -cne $HostSupervisorBuild.binary_target -or
        $HostLauncherBuild.package_name -cne 'eliot-swarm-controller' -or
        $HostLauncherBuild.binary_target -cne 'swarm-host' -or
        $HostBuild.package_name -cne 'swarm-kernel-host' -or
        $HostBuild.binary_target -cne 'swarm-kernel-host' -or
        $HostSupervisorBuild.package_name -cne 'swarm-supervisor' -or
        $HostSupervisorBuild.binary_target -cne 'swarm-supervisor' -or
        $PublicCliBuild.host_ipc_protocol_version -ne $HostBuild.host_ipc_protocol_version -or
        $PublicCliBuild.rustc_host_triple -cne $HostBuild.rustc_host_triple -or
        (Get-Sha256 $expectedLauncher) -cne $HostLauncherBuild.binary_sha256 -or
        (Get-Sha256 $expectedRuntime) -cne $HostBuild.binary_sha256 -or
        (Get-Sha256 $expectedSupervisor) -cne $HostSupervisorBuild.binary_sha256) {
        Stop-Harness 'PUBLIC_CLI_HOST_COORDINATE_MISMATCH'
    }
}

function Read-JsonFile {
    param([Parameter(Mandatory)][string] $Path)
    try { return (Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json -AsHashtable -Depth 48) }
    catch { Stop-Harness 'JSON_INPUT_INVALID' }
}

function Write-PrivateJson {
    param([Parameter(Mandatory)][string] $Path, [Parameter(Mandatory)] $Value)
    $bytes = [System.Text.UTF8Encoding]::new($false).GetBytes(($Value | ConvertTo-Json -Depth 48 -Compress))
    if ($bytes.Length -gt $script:MaxFrameBytes) { Stop-Harness 'REQUEST_FRAME_TOO_LARGE' }
    $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
    try { $stream.Write($bytes, 0, $bytes.Length); $stream.Flush($true) }
    finally { $stream.Dispose() }
}

function New-PrivateDirectory {
    param([Parameter(Mandatory)][string] $Path)
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
    [void][System.IO.FileSystemAclExtensions]::CreateDirectory($security, $Path)
    [void](Assert-SafeAbsolutePath -Path $Path -MustExist -Directory)
}

function Save-Summary {
    if ([string]::IsNullOrWhiteSpace($script:RunDirectory)) { return }
    $script:Summary.scenarios = @($script:Scenarios.ToArray())
    $target = Join-Path $script:RunDirectory 'qualification-receipt.json'
    $temporary = Join-Path $script:RunDirectory ('.receipt-' + [Guid]::NewGuid().ToString('N') + '.tmp')
    $bytes = [System.Text.UTF8Encoding]::new($false).GetBytes(($script:Summary | ConvertTo-Json -Depth 32))
    $stream = [System.IO.File]::Open($temporary, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
    try { $stream.Write($bytes, 0, $bytes.Length); $stream.Flush($true) }
    finally { $stream.Dispose() }
    [System.IO.File]::Move($temporary, $target, $true)
}

function Add-Scenario {
    param([Parameter(Mandatory)][System.Collections.IDictionary] $Scenario)
    $script:Scenarios.Add([pscustomobject]$Scenario)
    Save-Summary
}

function Get-SafeErrorCode {
    param([AllowNull()][string] $Text)
    if ([string]::IsNullOrWhiteSpace($Text)) { return 'CLI_EXIT' }
    foreach ($line in $Text -split "`n") {
        try {
            $value = $line | ConvertFrom-Json -AsHashtable -Depth 16
            if ($value -isnot [Collections.IDictionary] -or $value['error'] -isnot [Collections.IDictionary]) { continue }
            $errorValue = $value['error']
            $code = if ($errorValue['data'] -is [Collections.IDictionary]) { $errorValue['data']['code'] } else { $null }
            if ([string]::IsNullOrWhiteSpace([string]$code)) { $code = $errorValue['code'] }
            if ($code -is [string] -and $code -cmatch '\A[A-Z0-9_]{1,64}\z') { return $code }
        } catch { }
    }
    return 'CLI_EXIT'
}

function New-CaptureState {
    if (-not ('Eliot.Qualification.BoundedOutput' -as [type])) {
        Add-Type -TypeDefinition @'
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
namespace Eliot.Qualification {
    public sealed class BoundedOutput {
        public readonly StringBuilder Stdout = new StringBuilder();
        public readonly StringBuilder Stderr = new StringBuilder();
        public readonly object Sync = new object();
        public bool Overflow;
        public bool ReadFailed;
        private readonly int maximum;
        public BoundedOutput(int maximum) { this.maximum = maximum; }
        public async Task DrainAsync(StreamReader reader, bool stdout) {
            var buffer = new char[4096];
            var target = stdout ? Stdout : Stderr;
            while (true) {
                int count;
                try { count = await reader.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false); }
                catch (ObjectDisposedException) { lock (Sync) { ReadFailed = true; } return; }
                catch (IOException) { lock (Sync) { ReadFailed = true; } return; }
                if (count == 0) return;
                lock (Sync) {
                    int take = Math.Min(count, Math.Max(0, maximum - target.Length));
                    if (take > 0) target.Append(buffer, 0, take);
                    if (take < count) Overflow = true;
                }
            }
        }
    }
}
'@
    }
    return [Eliot.Qualification.BoundedOutput]::new($script:MaxCliOutputCharacters)
}

function Start-BoundedProcess {
    param([Parameter(Mandatory)][string[]] $Arguments, [Parameter(Mandatory)][int] $TimeoutMilliseconds, [string] $ExecutablePath = $script:PublicCliPath, [switch] $LongLived)
    $start = [System.Diagnostics.ProcessStartInfo]::new()
    if ([string]::IsNullOrWhiteSpace($ExecutablePath)) { Stop-Harness 'QUALIFICATION_BINARY_PATH_UNSET' }
    $start.FileName = $ExecutablePath
    $start.WorkingDirectory = Split-Path -LiteralPath $ExecutablePath
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.RedirectStandardInput = [bool]$LongLived
    foreach ($argument in $Arguments) { [void]$start.ArgumentList.Add([string]$argument) }
    $capture = New-CaptureState
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $start
    $processId = $null
    $processStarted = $false
    $drainTasks = [System.Threading.Tasks.Task[]]@()
    try {
        if (-not $process.Start()) { return [pscustomobject]@{ started = $false; completed = $true; exit_code = $null; error_code = 'PROCESS_START_FAILED'; stdout = ''; stderr = ''; process = $null; process_id = $null; overflow = $false } }
        $processStarted = $true
        $processId = [int]$process.Id
        $drainTasks = [System.Threading.Tasks.Task[]]@($capture.DrainAsync($process.StandardOutput, $true), $capture.DrainAsync($process.StandardError, $false))
        if ($LongLived) { return [pscustomobject]@{ started = $true; completed = $false; exit_code = $null; error_code = $null; stdout = ''; stderr = ''; process = $process; process_id = $processId; overflow = $false; capture = $capture; drain_tasks = $drainTasks } }
        $completed = $process.WaitForExit($TimeoutMilliseconds)
        if (-not $completed) {
            $script:PendingPublicCliPids.Add($processId)
            return [pscustomobject]@{ started = $true; completed = $false; exit_code = $null; error_code = 'CLI_TIMEOUT_OUTCOME_UNKNOWN'; stdout = ''; stderr = ''; process = $null; process_id = $processId; overflow = $false }
        }
        if (-not [System.Threading.Tasks.Task]::WaitAll($drainTasks, 1000)) {
            return [pscustomobject]@{ started = $false; completed = $true; exit_code = $null; error_code = 'CLI_OUTPUT_DRAIN_PENDING'; stdout = ''; stderr = ''; process = $null; process_id = $processId; overflow = $false }
        }
        if ($capture.ReadFailed) {
            return [pscustomobject]@{ started = $false; completed = $true; exit_code = $null; error_code = 'PROCESS_IO_FAILED'; stdout = ''; stderr = ''; process = $null; process_id = $processId; overflow = $false }
        }
        [System.Threading.Monitor]::Enter($capture.sync)
        try {
            $stdout = $capture.stdout.ToString()
            $stderr = $capture.stderr.ToString()
            $overflow = [bool]$capture.overflow
        }
        finally { [System.Threading.Monitor]::Exit($capture.sync) }
        return [pscustomobject]@{ started = $true; completed = $true; exit_code = $process.ExitCode; error_code = $(if ($process.ExitCode -eq 0) { $null } else { Get-SafeErrorCode $stderr }); stdout = $stdout; stderr = $stderr; process = $null; process_id = $processId; overflow = $overflow }
    }
    catch {
        if ($processStarted) {
            if ($null -eq $processId) { try { $processId = [int]$process.Id } catch { } }
            $stillRunning = $true
            try { $stillRunning = -not $process.HasExited } catch { $stillRunning = $true }
            if ($stillRunning -and $LongLived) {
                return [pscustomobject]@{ started = $true; completed = $false; exit_code = $null; error_code = 'PROCESS_IO_FAILED'; stdout = ''; stderr = ''; process = $process; process_id = $processId; overflow = $false; capture = $capture; drain_tasks = $drainTasks }
            }
            if ($stillRunning) {
                if ($null -ne $processId -and -not $script:PendingPublicCliPids.Contains($processId)) { $script:PendingPublicCliPids.Add($processId) }
                return [pscustomobject]@{ started = $true; completed = $false; exit_code = $null; error_code = 'CLI_TIMEOUT_OUTCOME_UNKNOWN'; stdout = ''; stderr = ''; process = $null; process_id = $processId; overflow = $false }
            }
            if ($LongLived) { return [pscustomobject]@{ started = $false; completed = $true; exit_code = $null; error_code = 'PROCESS_IO_FAILED'; stdout = ''; stderr = ''; process = $null; process_id = $processId; overflow = $false } }
            return [pscustomobject]@{ started = $true; completed = $true; exit_code = $null; error_code = 'PROCESS_IO_FAILED'; stdout = ''; stderr = ''; process = $null; process_id = $processId; overflow = $false }
        }
        return [pscustomobject]@{ started = $false; completed = $true; exit_code = $null; error_code = 'PROCESS_IO_FAILED'; stdout = ''; stderr = ''; process = $null; process_id = $null; overflow = $false }
    }
    finally {
        $disposeProcess = -not $LongLived -or -not $processStarted
        if (-not $disposeProcess) {
            try { $disposeProcess = $process.HasExited } catch { $disposeProcess = $false }
        }
        if ($disposeProcess) { $process.Dispose() }
    }
}

function Read-RpcResponse {
    param(
        [Parameter(Mandatory)][System.IO.StreamReader] $Reader,
        [Parameter(Mandatory)][string] $ExpectedId,
        [Parameter(Mandatory)][System.Threading.CancellationToken] $CancellationToken
    )
    try { $line = $Reader.ReadLineAsync($CancellationToken).GetAwaiter().GetResult() }
    catch [System.OperationCanceledException] { return [pscustomobject]@{ success = $false; code = 'RPC_READ_TIMEOUT'; value = $null; response_received = $false } }
    catch [System.IO.IOException] { return [pscustomobject]@{ success = $false; code = 'RPC_READ_FAILED'; value = $null; response_received = $false } }
    if ($null -eq $line) { return [pscustomobject]@{ success = $false; code = 'RPC_RESPONSE_INVALID'; value = $null; response_received = $false } }
    if ([string]::IsNullOrWhiteSpace($line) -or [System.Text.Encoding]::UTF8.GetByteCount($line) -gt $script:MaxFrameBytes) {
        return [pscustomobject]@{ success = $false; code = 'RPC_RESPONSE_INVALID'; value = $null; response_received = $true }
    }
    try { $reply = $line | ConvertFrom-Json -AsHashtable -Depth 32 }
    catch { return [pscustomobject]@{ success = $false; code = 'RPC_RESPONSE_INVALID'; value = $null; response_received = $true } }
    if ($reply -isnot [Collections.IDictionary] -or $reply['jsonrpc'] -cne '2.0' -or $reply['id'] -cne $ExpectedId) {
        return [pscustomobject]@{ success = $false; code = 'RPC_RESPONSE_MISMATCH'; value = $null; response_received = $true }
    }
    if ($reply.Contains('error')) {
        $errorValue = $reply['error']
        $errorData = if ($errorValue -is [Collections.IDictionary]) { $errorValue['data'] } else { $null }
        $code = if ($errorData -is [Collections.IDictionary]) { $errorData['code'] } else { $null }
        if ($code -isnot [string] -or $code -cnotmatch '\A[A-Z0-9_]{1,64}\z') { $code = 'RPC_ERROR' }
        return [pscustomobject]@{ success = $false; code = $code; value = $null; response_received = $true }
    }
    if (-not $reply.Contains('result')) { return [pscustomobject]@{ success = $false; code = 'RPC_RESPONSE_INVALID'; value = $null; response_received = $true } }
    return [pscustomobject]@{ success = $true; code = $null; value = $reply['result']; response_received = $true }
}

function Invoke-DroppedReplyRpc {
    param(
        [Parameter(Mandatory)][string] $DataDirectory,
        [Parameter(Mandatory)][ValidatePattern('\Aeliot-swarm-[a-f0-9]{32}\z')][string] $PipeName,
        [Parameter(Mandatory)][System.Collections.IDictionary] $Credential,
        [Parameter(Mandatory)][string] $Method,
        [Parameter(Mandatory)][System.Collections.IDictionary] $Params
    )
    if ($Method -notin @('task.create', 'hook.emit')) { Stop-Harness 'FAULT_TRANSPORT_METHOD_NOT_ALLOWED' }
    $pipe = $null
    $reader = $null
    $cancellation = $null
    $applicationAttempted = $false
    $stage = 'CONNECT'
    $helloId = [Guid]::NewGuid().ToString('D')
    $requestId = [Guid]::NewGuid().ToString('D')
    $ioTimeoutMilliseconds = [Math]::Min(10000, $TimeoutSeconds * 1000)
    try {
        $pipe = [System.IO.Pipes.NamedPipeClientStream]::new('.', $PipeName, [System.IO.Pipes.PipeDirection]::InOut, [System.IO.Pipes.PipeOptions]::Asynchronous)
        $cancellation = [System.Threading.CancellationTokenSource]::new([TimeSpan]::FromMilliseconds($ioTimeoutMilliseconds))
        $pipe.ConnectAsync($ioTimeoutMilliseconds, $cancellation.Token).GetAwaiter().GetResult()
        $cancellation.Dispose(); $cancellation = $null
        $encoding = [System.Text.UTF8Encoding]::new($false)
        $reader = [System.IO.StreamReader]::new($pipe, $encoding, $false, 4096, $true)
        $hello = [ordered]@{ jsonrpc = '2.0'; id = $helloId; method = 'client.hello'; params = $Credential }
        $helloLine = $hello | ConvertTo-Json -Depth 24 -Compress
        if ([System.Text.Encoding]::UTF8.GetByteCount($helloLine) -gt $script:MaxFrameBytes) { return [pscustomobject]@{ sent = $false; effect_possible = $false; response_known = $true; success = $false; code = 'RPC_FRAME_TOO_LARGE'; value = $null } }
        $helloBytes = $encoding.GetBytes($helloLine + [Environment]::NewLine)
        $stage = 'AUTH_WRITE'
        $cancellation = [System.Threading.CancellationTokenSource]::new([TimeSpan]::FromMilliseconds($ioTimeoutMilliseconds))
        $pipe.WriteAsync($helloBytes, 0, $helloBytes.Length, $cancellation.Token).GetAwaiter().GetResult()
        $cancellation.Dispose(); $cancellation = $null
        $stage = 'AUTH_READ'
        $cancellation = [System.Threading.CancellationTokenSource]::new([TimeSpan]::FromMilliseconds($ioTimeoutMilliseconds))
        $helloReply = Read-RpcResponse -Reader $reader -ExpectedId $helloId -CancellationToken $cancellation.Token
        $cancellation.Dispose(); $cancellation = $null
        $helloClientId = $null
        if ($helloReply.success -and $helloReply.value -is [Collections.IDictionary]) { $helloClientId = [string]$helloReply.value['client_id'] }
        if (-not $helloReply.success -or $helloClientId -cne [string]$Credential.client_id) {
            return [pscustomobject]@{ sent = $false; effect_possible = $false; response_known = [bool]$helloReply.response_received; success = $false; code = $(if ($helloReply.code) { $helloReply.code } else { 'RPC_AUTH_MISMATCH' }); value = $null }
        }
        $request = [ordered]@{ jsonrpc = '2.0'; id = $requestId; method = $Method; params = $Params }
        $requestLine = $request | ConvertTo-Json -Depth 48 -Compress
        if ([System.Text.Encoding]::UTF8.GetByteCount($requestLine) -gt $script:MaxFrameBytes) { return [pscustomobject]@{ sent = $false; effect_possible = $false; response_known = $true; success = $false; code = 'RPC_FRAME_TOO_LARGE'; value = $null } }
        $requestBytes = $encoding.GetBytes($requestLine + [Environment]::NewLine)
        $stage = 'APPLICATION_WRITE'
        $cancellation = [System.Threading.CancellationTokenSource]::new([TimeSpan]::FromMilliseconds($ioTimeoutMilliseconds))
        $applicationAttempted = $true
        $pipe.WriteAsync($requestBytes, 0, $requestBytes.Length, $cancellation.Token).GetAwaiter().GetResult()
        $cancellation.Dispose(); $cancellation = $null
        $reader.Dispose(); $reader = $null
        $pipe.Dispose(); $pipe = $null
        return [pscustomobject]@{ sent = $true; effect_possible = $true; response_known = $false; success = $false; code = 'ACK_DROPPED_BY_HARNESS'; value = $null }
    }
    catch [System.OperationCanceledException] {
        return [pscustomobject]@{ sent = $applicationAttempted; effect_possible = $applicationAttempted; response_known = $false; success = $false; code = $(if ($applicationAttempted) { 'RPC_OUTCOME_UNKNOWN' } else { 'RPC_' + $stage + '_TIMEOUT' }); value = $null }
    }
    catch [System.TimeoutException] {
        return [pscustomobject]@{ sent = $applicationAttempted; effect_possible = $applicationAttempted; response_known = $false; success = $false; code = $(if ($applicationAttempted) { 'RPC_OUTCOME_UNKNOWN' } else { 'RPC_' + $stage + '_TIMEOUT' }); value = $null }
    }
    catch {
        return [pscustomobject]@{ sent = $applicationAttempted; effect_possible = $applicationAttempted; response_known = $false; success = $false; code = $(if ($applicationAttempted) { 'RPC_OUTCOME_UNKNOWN' } elseif ($stage -eq 'CONNECT') { 'RPC_CONNECT_FAILED' } else { 'RPC_' + $stage + '_FAILED' }); value = $null }
    }
    finally {
        if ($null -ne $cancellation) { $cancellation.Dispose() }
        if ($null -ne $reader) { $reader.Dispose() }
        if ($null -ne $pipe) { $pipe.Dispose() }
    }
}

function New-ScenarioDirectory {
    param([Parameter(Mandatory)][string] $Name)
    $path = Join-Path $script:RunDirectory ($Name + '-' + [Guid]::NewGuid().ToString('N'))
    New-PrivateDirectory -Path $path
    $state = Join-Path $path 'state'
    $requests = Join-Path $path 'requests'
    New-PrivateDirectory -Path $state
    New-PrivateDirectory -Path $requests
    return [pscustomobject]@{ name = $Name; path = $path; state = $state; requests = $requests; host = $null; pipe_name = $null; operator_path = (Join-Path $state 'operator.json'); hook_credential_path = (Join-Path $path 'hook-credential.json') }
}

function Start-IsolatedHost {
    param([Parameter(Mandatory)] $Scenario)
    $arguments = @('--config', $script:ConfigPath, '--data-dir', $Scenario.state, 'host', '--stop-on-stdin-eof')
    $result = Start-BoundedProcess -Arguments $arguments -TimeoutMilliseconds 0 -ExecutablePath $script:HostPath -LongLived
    if (-not $result.started) { return [pscustomobject]@{ ready = $false; code = $result.error_code; host_pid = $null } }
    $Scenario.host = $result.process
    $deadline = [DateTime]::UtcNow.AddSeconds([Math]::Min(30, $TimeoutSeconds))
    while ([DateTime]::UtcNow -lt $deadline) {
        if ($Scenario.host.HasExited) {
            $drainCompleted = $false
            try { $drainCompleted = [System.Threading.Tasks.Task]::WaitAll($result.drain_tasks, 1000) } catch { }
            $capturePath = Join-Path $Scenario.path ('host-start-' + [Guid]::NewGuid().ToString('N') + '.json')
            [System.Threading.Monitor]::Enter($result.capture.sync)
            try {
                Write-PrivateJson -Path $capturePath -Value ([ordered]@{
                    exit_code = $Scenario.host.ExitCode
                    capture_complete = $drainCompleted -and -not $result.capture.ReadFailed -and -not $result.capture.overflow
                    drain_completed = $drainCompleted
                    read_failed = [bool]$result.capture.ReadFailed
                    overflow = [bool]$result.capture.overflow
                    stdout = $result.capture.stdout.ToString()
                    stderr = $result.capture.stderr.ToString()
                })
            } finally { [System.Threading.Monitor]::Exit($result.capture.sync) }
            return [pscustomobject]@{ ready = $false; code = 'HOST_EXITED_DURING_START'; host_pid = $Scenario.host.Id }
        }
        if (Test-Path -LiteralPath $Scenario.operator_path) {
            $credential = Read-JsonFile $Scenario.operator_path
            $status = Invoke-SwarmCall -Scenario $Scenario -CredentialPath $Scenario.operator_path -Method 'host.status' -Params ([ordered]@{})
            if ($status.success -and $null -ne $status.value) {
                [System.Threading.Monitor]::Enter($result.capture.sync)
                try { $hostStderr = $result.capture.stderr.ToString() }
                finally { [System.Threading.Monitor]::Exit($result.capture.sync) }
                $prefix = 'swarm host ready: \\.\pipe\'
                $pipeNames = @($hostStderr -split "`n" | ForEach-Object {
                    $line = $_.TrimEnd("`r")
                    if ($line.StartsWith($prefix, [StringComparison]::Ordinal)) {
                        $name = $line.Substring($prefix.Length)
                        if ($name -cmatch '\Aeliot-swarm-[a-f0-9]{32}\z') { $name }
                    }
                })
                if ($pipeNames.Count -eq 1) {
                    $Scenario.pipe_name = $pipeNames[0]
                    return [pscustomobject]@{ ready = $true; code = $null; host_pid = $Scenario.host.Id; epoch = [long]$status.value.host_epoch }
                }
            }
        }
        Start-Sleep -Milliseconds 250
    }
    return [pscustomobject]@{ ready = $false; code = 'HOST_READY_TIMEOUT'; host_pid = $Scenario.host.Id }
}

function Stop-IsolatedHost {
    param([Parameter(Mandatory)] $Scenario)
    if ($null -eq $Scenario.host) { return [pscustomobject]@{ stopped = $true; pending_host_pid = $null } }
    $process = $Scenario.host
    try {
        if (-not $process.HasExited) {
            $process.StandardInput.Close()
            if (-not $process.WaitForExit([Math]::Min(30000, $TimeoutSeconds * 1000))) {
                $script:PendingHostPids.Add([int]$process.Id)
                return [pscustomobject]@{ stopped = $false; pending_host_pid = [int]$process.Id }
            }
        }
        return [pscustomobject]@{ stopped = $true; pending_host_pid = $null }
    }
    catch {
        if (-not $process.HasExited) { $script:PendingHostPids.Add([int]$process.Id); return [pscustomobject]@{ stopped = $false; pending_host_pid = [int]$process.Id } }
        return [pscustomobject]@{ stopped = $true; pending_host_pid = $null }
    }
    finally {
        if ($process.HasExited) { $process.Dispose(); $Scenario.host = $null }
    }
}

function Invoke-SwarmCall {
    param(
        [Parameter(Mandatory)] $Scenario,
        [Parameter(Mandatory)][string] $CredentialPath,
        [Parameter(Mandatory)][string] $Method,
        [Parameter(Mandatory)][System.Collections.IDictionary] $Params,
        [int] $TimeoutMilliseconds = 15000
    )
    $requestPath = Join-Path $Scenario.requests ([Guid]::NewGuid().ToString('N') + '.json')
    Write-PrivateJson -Path $requestPath -Value $Params
    $arguments = @('--config', $script:ConfigPath, '--data-dir', $Scenario.state, '--credential', $CredentialPath, 'call', $Method, '--file', $requestPath)
    $process = Start-BoundedProcess -Arguments $arguments -TimeoutMilliseconds $TimeoutMilliseconds
    if ($process.completed) { try { [System.IO.File]::Delete($requestPath) } catch { } }
    if (-not $process.completed) { return [pscustomobject]@{ completed = $false; success = $false; code = $process.error_code; value = $null; public_cli_pid = $process.process_id } }
    if (-not $process.started -or $process.overflow) { return [pscustomobject]@{ completed = $true; success = $false; code = $(if ($process.overflow) { 'CLI_OUTPUT_LIMIT' } else { $process.error_code }); value = $null; public_cli_pid = $process.process_id } }
    if ($process.exit_code -ne 0) {
        Write-PrivateJson -Path (Join-Path $Scenario.path ('cli-error-' + [Guid]::NewGuid().ToString('N') + '.json')) -Value ([ordered]@{ method = $Method; exit_code = $process.exit_code; stdout = $process.stdout; stderr = $process.stderr })
        return [pscustomobject]@{ completed = $true; success = $false; code = $process.error_code; value = $null; public_cli_pid = $process.process_id }
    }
    try { $value = $process.stdout | ConvertFrom-Json -AsHashtable -Depth 48 }
    catch { return [pscustomobject]@{ completed = $true; success = $false; code = 'CLI_RESULT_INVALID'; value = $null; public_cli_pid = $process.process_id } }
    return [pscustomobject]@{ completed = $true; success = $true; code = $null; value = $value; public_cli_pid = $process.process_id }
}

function New-Manager {
    param([Parameter(Mandatory)] $Scenario)
    $managerId = 'core-failure-' + [Guid]::NewGuid().ToString('N')
    $credentialPath = Join-Path $Scenario.path 'manager.json'
    $arguments = @('--config', $script:ConfigPath, '--data-dir', $Scenario.state, '--credential', $Scenario.operator_path,
        'client-create', $managerId, '--role', 'manager', '--out', $credentialPath)
    $result = Start-BoundedProcess -Arguments $arguments -TimeoutMilliseconds 15000
    if (-not $result.completed -or $result.exit_code -ne 0 -or -not (Test-Path -LiteralPath $credentialPath)) { return [pscustomobject]@{ success = $false; code = $(if ($result.error_code) { $result.error_code } else { 'MANAGER_CREATE_UNKNOWN' }); id = $managerId; path = $credentialPath; credential = $null } }
    $credential = Read-JsonFile $credentialPath
    if ($credential.client_id -cne $managerId -or [string]::IsNullOrWhiteSpace([string]$credential.token)) { return [pscustomobject]@{ success = $false; code = 'MANAGER_CREDENTIAL_INVALID'; id = $managerId; path = $credentialPath; credential = $null } }
    $handover = Invoke-SwarmCall -Scenario $Scenario -CredentialPath $Scenario.operator_path -Method 'gm.handover' -Params ([ordered]@{ client_request_id = 'core-gm:' + [Guid]::NewGuid().ToString('N'); client_id = $managerId })
    if (-not $handover.success) { return [pscustomobject]@{ success = $false; code = $(if ($handover.code) { $handover.code } else { 'GM_DESIGNATION_UNKNOWN' }); id = $managerId; path = $credentialPath; credential = $null } }
    $status = Invoke-SwarmCall -Scenario $Scenario -CredentialPath $credentialPath -Method 'host.status' -Params ([ordered]@{})
    if (-not $status.success -or $status.value['gm'] -isnot [Collections.IDictionary] -or
        $status.value.gm['state'] -cne 'current' -or $status.value.gm['client_id'] -cne $managerId -or
        $status.value.gm['epoch'] -ne $handover.value['gm_epoch'] -or $status.value.gm['epoch'] -le 0) {
        return [pscustomobject]@{ success = $false; code = 'GM_DESIGNATION_READBACK_MISMATCH'; id = $managerId; path = $credentialPath; credential = $null }
    }
    return [pscustomobject]@{ success = $true; code = $null; id = $managerId; path = $credentialPath; credential = $credential; gm_epoch = [long]$status.value.gm['epoch'] }
}

function Test-JsonNumberValue {
    param([Parameter(Mandatory)] $Value)
    if ($Value -is [bool]) { return $false }
    $typeCode = [System.Type]::GetTypeCode($Value.GetType())
    return $typeCode -in @(
        [System.TypeCode]::SByte, [System.TypeCode]::Byte,
        [System.TypeCode]::Int16, [System.TypeCode]::UInt16,
        [System.TypeCode]::Int32, [System.TypeCode]::UInt32,
        [System.TypeCode]::Int64, [System.TypeCode]::UInt64,
        [System.TypeCode]::Single, [System.TypeCode]::Double,
        [System.TypeCode]::Decimal
    )
}

function Test-JsonNumberEqual {
    param([Parameter(Mandatory)] $Left, [Parameter(Mandatory)] $Right)
    $invariant = [System.Globalization.CultureInfo]::InvariantCulture
    $leftText = [System.Convert]::ToString($Left, $invariant)
    $rightText = [System.Convert]::ToString($Right, $invariant)
    $leftDecimal = [decimal]::Zero
    $rightDecimal = [decimal]::Zero
    $leftFitsDecimal = [decimal]::TryParse($leftText, [System.Globalization.NumberStyles]::Float, $invariant, [ref]$leftDecimal)
    $rightFitsDecimal = [decimal]::TryParse($rightText, [System.Globalization.NumberStyles]::Float, $invariant, [ref]$rightDecimal)
    if ($leftFitsDecimal -and $rightFitsDecimal) { return $leftDecimal -eq $rightDecimal }
    $leftDouble = [double]::NaN
    $rightDouble = [double]::NaN
    $leftFitsDouble = [double]::TryParse($leftText, [System.Globalization.NumberStyles]::Float, $invariant, [ref]$leftDouble)
    $rightFitsDouble = [double]::TryParse($rightText, [System.Globalization.NumberStyles]::Float, $invariant, [ref]$rightDouble)
    return $leftFitsDouble -and $rightFitsDouble -and -not [double]::IsNaN($leftDouble) -and
        -not [double]::IsInfinity($leftDouble) -and -not [double]::IsNaN($rightDouble) -and
        -not [double]::IsInfinity($rightDouble) -and $leftDouble -eq $rightDouble
}

function Test-JsonSemanticEqual {
    param($Left, $Right)
    if ($null -eq $Left -or $null -eq $Right) { return $null -eq $Left -and $null -eq $Right }
    if ($Left -is [System.Collections.IDictionary] -or $Right -is [System.Collections.IDictionary]) {
        if ($Left -isnot [System.Collections.IDictionary] -or $Right -isnot [System.Collections.IDictionary] -or
            $Left.Count -ne $Right.Count) { return $false }
        foreach ($leftKey in $Left.Keys) {
            $rightKeyMatch = $null
            $keyMatches = 0
            foreach ($rightKey in $Right.Keys) {
                if ([string]::Equals([string]$leftKey, [string]$rightKey, [StringComparison]::Ordinal)) {
                    $rightKeyMatch = $rightKey
                    $keyMatches++
                }
            }
            if ($keyMatches -ne 1 -or -not (Test-JsonSemanticEqual -Left $Left[$leftKey] -Right $Right[$rightKeyMatch])) { return $false }
        }
        return $true
    }
    if ($Left -is [System.Collections.IList] -or $Right -is [System.Collections.IList]) {
        if ($Left -isnot [System.Collections.IList] -or $Right -isnot [System.Collections.IList] -or
            $Left.Count -ne $Right.Count) { return $false }
        for ($index = 0; $index -lt $Left.Count; $index++) {
            if (-not (Test-JsonSemanticEqual -Left $Left[$index] -Right $Right[$index])) { return $false }
        }
        return $true
    }
    $leftIsNumber = Test-JsonNumberValue -Value $Left
    $rightIsNumber = Test-JsonNumberValue -Value $Right
    if ($leftIsNumber -or $rightIsNumber) {
        return $leftIsNumber -and $rightIsNumber -and (Test-JsonNumberEqual -Left $Left -Right $Right)
    }
    if ($Left -is [string] -or $Right -is [string]) {
        return $Left -is [string] -and $Right -is [string] -and [string]::Equals($Left, $Right, [StringComparison]::Ordinal)
    }
    if ($Left -is [bool] -or $Right -is [bool]) { return $Left -is [bool] -and $Right -is [bool] -and $Left -eq $Right }
    return [object]::Equals($Left, $Right)
}

function ConvertTo-StrictInt64 {
    param($Value)
    if ($null -eq $Value -or $Value -is [bool]) { return [pscustomobject]@{ valid = $false; value = $null } }
    $typeCode = [System.Type]::GetTypeCode($Value.GetType())
    if ($typeCode -notin @(
        [System.TypeCode]::SByte, [System.TypeCode]::Byte,
        [System.TypeCode]::Int16, [System.TypeCode]::UInt16,
        [System.TypeCode]::Int32, [System.TypeCode]::UInt32,
        [System.TypeCode]::Int64, [System.TypeCode]::UInt64
    )) { return [pscustomobject]@{ valid = $false; value = $null } }
    try { return [pscustomobject]@{ valid = $true; value = [long]$Value } }
    catch { return [pscustomobject]@{ valid = $false; value = $null } }
}

function Test-PublicTaskMatchesFixture {
    param($Task, [Parameter(Mandatory)][string] $TaskId, [Parameter(Mandatory)][string] $ExpectedProjectId,
        [Parameter(Mandatory)][System.Collections.IDictionary] $ExpectedSpec)
    if ($Task -isnot [System.Collections.IDictionary] -or
        -not $Task.Contains('task_id') -or -not $Task.Contains('project_id') -or -not $Task.Contains('spec')) { return $false }
    return $Task['task_id'] -is [string] -and $Task['task_id'] -ceq $TaskId -and
        $Task['project_id'] -is [string] -and $Task['project_id'] -ceq $ExpectedProjectId -and
        (Test-JsonSemanticEqual -Left $Task['spec'] -Right $ExpectedSpec)
}

function Read-PublicListPages {
    param([Parameter(Mandatory)] $Scenario, [Parameter(Mandatory)][string] $CredentialPath,
        [Parameter(Mandatory)][ValidateSet('task.list', 'operation.list')][string] $Method,
        [Parameter(Mandatory)][ValidateSet('task_id', 'operation_id')][string] $IdentityField,
        [datetime] $DeadlineUtc = [DateTime]::MinValue)
    if ($DeadlineUtc -eq [DateTime]::MinValue) { $DeadlineUtc = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds) }
    $items = [System.Collections.Generic.List[object]]::new()
    $seenIdentities = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
    $after = [long]0
    $pageLimit = 50
    $maximumPages = 512
    for ($pageNumber = 1; $pageNumber -le $maximumPages; $pageNumber++) {
        $remainingMilliseconds = ($DeadlineUtc - [DateTime]::UtcNow).TotalMilliseconds
        if ($remainingMilliseconds -le 0) {
            return [pscustomobject]@{ success = $false; code = 'PUBLIC_LIST_PAGINATION_TIMEOUT'; items = @(); pages = $pageNumber - 1 }
        }
        $pageTimeoutMilliseconds = [int][Math]::Max(1, [Math]::Min(10000, [Math]::Floor($remainingMilliseconds)))
        $call = Invoke-SwarmCall -Scenario $Scenario -CredentialPath $CredentialPath -Method $Method `
            -Params ([ordered]@{ after = $after; limit = $pageLimit }) -TimeoutMilliseconds $pageTimeoutMilliseconds
        if (-not $call.success) {
            return [pscustomobject]@{ success = $false; code = $(if ($call.code) { $call.code } else { 'PUBLIC_LIST_READ_FAILED' }); items = @(); pages = $pageNumber }
        }
        $page = $call.value
        if ($page -isnot [System.Collections.IDictionary] -or
            -not $page.Contains('items') -or -not $page.Contains('next_after') -or
            -not $page.Contains('pagination') -or -not $page.Contains('examined_through') -or
            -not $page.Contains('has_newer') -or -not $page.Contains('coverage_complete') -or
            -not $page.Contains('gap_count') -or $page['items'] -isnot [System.Collections.IList] -or
            $page['pagination'] -cne 'committed_observation_position' -or
            $page['has_newer'] -isnot [bool] -or $page['coverage_complete'] -isnot [bool] -or
            $page['coverage_complete'] -ne $true) {
            return [pscustomobject]@{ success = $false; code = 'PUBLIC_LIST_PAGE_MALFORMED'; items = @(); pages = $pageNumber }
        }
        $nextCursor = ConvertTo-StrictInt64 -Value $page['next_after']
        $examinedCursor = ConvertTo-StrictInt64 -Value $page['examined_through']
        $gapCount = ConvertTo-StrictInt64 -Value $page['gap_count']
        if (-not $nextCursor.valid -or -not $examinedCursor.valid -or -not $gapCount.valid -or
            $nextCursor.value -lt $after -or $examinedCursor.value -ne $nextCursor.value -or
            $gapCount.value -ne 0) {
            return [pscustomobject]@{ success = $false; code = 'PUBLIC_LIST_CURSOR_OR_COVERAGE_INVALID'; items = @(); pages = $pageNumber }
        }
        $pageItems = $page['items']
        if ($pageItems.Count -gt $pageLimit) {
            return [pscustomobject]@{ success = $false; code = 'PUBLIC_LIST_PAGE_OVERSIZED'; items = @(); pages = $pageNumber }
        }
        $previousItemCursor = $after
        foreach ($item in $pageItems) {
            if ($item -isnot [System.Collections.IDictionary] -or $item.Contains('gap') -or
                -not $item.Contains('cursor') -or -not $item.Contains($IdentityField)) {
                return [pscustomobject]@{ success = $false; code = 'PUBLIC_LIST_ENTRY_MALFORMED'; items = @(); pages = $pageNumber }
            }
            $itemCursor = ConvertTo-StrictInt64 -Value $item['cursor']
            $identity = $item[$IdentityField]
            if (-not $itemCursor.valid -or $itemCursor.value -le $previousItemCursor -or
                $itemCursor.value -gt $nextCursor.value -or $identity -isnot [string] -or
                [string]::IsNullOrWhiteSpace($identity) -or -not $seenIdentities.Add($identity)) {
                return [pscustomobject]@{ success = $false; code = 'PUBLIC_LIST_ENTRY_CURSOR_OR_IDENTITY_INVALID'; items = @(); pages = $pageNumber }
            }
            if ($Method -ceq 'operation.list' -and
                (-not $item.Contains('method') -or $item['method'] -isnot [string] -or [string]::IsNullOrWhiteSpace($item['method']))) {
                return [pscustomobject]@{ success = $false; code = 'PUBLIC_OPERATION_ENTRY_MALFORMED'; items = @(); pages = $pageNumber }
            }
            $previousItemCursor = $itemCursor.value
            $items.Add($item)
        }
        if ($page['has_newer'] -eq $true) {
            if ($nextCursor.value -le $after) {
                return [pscustomobject]@{ success = $false; code = 'PUBLIC_LIST_CURSOR_NO_PROGRESS'; items = @(); pages = $pageNumber }
            }
            $after = $nextCursor.value
            continue
        }
        return [pscustomobject]@{ success = $true; code = $null; items = [object[]]$items.ToArray(); pages = $pageNumber }
    }
    return [pscustomobject]@{ success = $false; code = 'PUBLIC_LIST_PAGE_LIMIT_REACHED'; items = @(); pages = $maximumPages }
}

function Read-TaskCreateAdmission {
    param([Parameter(Mandatory)] $Scenario, [Parameter(Mandatory)] $Manager,
        [Parameter(Mandatory)][string] $ExpectedProjectId,
        [Parameter(Mandatory)][System.Collections.IDictionary] $ExpectedSpec)
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        $listed = Read-PublicListPages -Scenario $Scenario -CredentialPath $Manager.path -Method 'operation.list' `
            -IdentityField 'operation_id' -DeadlineUtc $deadline
        if (-not $listed.success) { return [pscustomobject]@{ success = $false; code = $listed.code; operation_id = $null; task_id = $null; operation = $null; task = $null } }
        $matches = @($listed.items | Where-Object { $_['method'] -ceq 'task.create' })
        if ($matches.Count -gt 1) {
            return [pscustomobject]@{ success = $false; code = 'TASK_CREATE_OPERATION_AMBIGUOUS'; operation_id = $null; task_id = $null; operation = $null; task = $null }
        }
        if ($matches.Count -eq 1) {
            $listedOperation = $matches[0]
            if (-not $listedOperation.Contains('task_id') -or $listedOperation['task_id'] -isnot [string] -or
                [string]::IsNullOrWhiteSpace($listedOperation['operation_id']) -or
                [string]::IsNullOrWhiteSpace($listedOperation['task_id'])) {
                return [pscustomobject]@{ success = $false; code = 'TASK_CREATE_PUBLIC_IDENTITY_MISSING'; operation_id = $null; task_id = $null; operation = $null; task = $null }
            }
            $operationId = [string]$listedOperation['operation_id']
            $taskId = [string]$listedOperation['task_id']
            $remainingMilliseconds = ($deadline - [DateTime]::UtcNow).TotalMilliseconds
            if ($remainingMilliseconds -le 0) {
                return [pscustomobject]@{ success = $false; code = 'TASK_CREATE_EXACT_READ_TIMEOUT'; operation_id = $operationId; task_id = $taskId; operation = $null; task = $null }
            }
            $exactReadTimeoutMilliseconds = [int][Math]::Max(1, [Math]::Min(10000, [Math]::Floor($remainingMilliseconds)))
            $operationRead = Invoke-SwarmCall -Scenario $Scenario -CredentialPath $Manager.path -Method 'operation.get' `
                -Params ([ordered]@{ operation_id = $operationId }) -TimeoutMilliseconds $exactReadTimeoutMilliseconds
            $remainingMilliseconds = ($deadline - [DateTime]::UtcNow).TotalMilliseconds
            if ($remainingMilliseconds -le 0) {
                return [pscustomobject]@{ success = $false; code = 'TASK_CREATE_EXACT_READ_TIMEOUT'; operation_id = $operationId; task_id = $taskId; operation = $null; task = $null }
            }
            $exactReadTimeoutMilliseconds = [int][Math]::Max(1, [Math]::Min(10000, [Math]::Floor($remainingMilliseconds)))
            $taskRead = Invoke-SwarmCall -Scenario $Scenario -CredentialPath $Manager.path -Method 'task.get' `
                -Params ([ordered]@{ task_id = $taskId }) -TimeoutMilliseconds $exactReadTimeoutMilliseconds
            if (-not $operationRead.success -or -not $taskRead.success) {
                return [pscustomobject]@{ success = $false; code = $(if ($operationRead.code) { $operationRead.code } elseif ($taskRead.code) { $taskRead.code } else { 'TASK_CREATE_EXACT_READ_FAILED' }); operation_id = $operationId; task_id = $taskId; operation = $null; task = $null }
            }
            $operation = $operationRead.value
            $task = $taskRead.value
            if ($operation -isnot [System.Collections.IDictionary] -or
                -not $operation.Contains('operation_id') -or -not $operation.Contains('method') -or
                -not $operation.Contains('state') -or -not $operation.Contains('task_id') -or
                -not $operation.Contains('result') -or $operation['result'] -isnot [System.Collections.IDictionary] -or
                $operation['operation_id'] -cne $operationId -or $operation['method'] -cne 'task.create' -or
                $operation['state'] -cne 'settled' -or $operation['task_id'] -cne $taskId -or
                $operation['result']['task_id'] -cne $taskId -or
                -not (Test-PublicTaskMatchesFixture -Task $task -TaskId $taskId -ExpectedProjectId $ExpectedProjectId -ExpectedSpec $ExpectedSpec)) {
                return [pscustomobject]@{ success = $false; code = 'TASK_CREATE_PRE_RESTART_RECEIPT_OR_TASK_MISMATCH'; operation_id = $operationId; task_id = $taskId; operation = $operation; task = $task }
            }
            return [pscustomobject]@{ success = $true; code = $null; operation_id = $operationId; task_id = $taskId; operation = $operation; task = $task; pages = $listed.pages }
        }
        Start-Sleep -Milliseconds 300
    }
    return [pscustomobject]@{ success = $false; code = 'PRE_RESTART_TASK_CREATE_ADMISSION_NOT_OBSERVED'; operation_id = $null; task_id = $null; operation = $null; task = $null }
}

function Read-PublicHookSourceEvents {
    param([Parameter(Mandatory)] $Scenario, [Parameter(Mandatory)][string] $CredentialPath,
        [Parameter(Mandatory)][string] $SourceId, [datetime] $DeadlineUtc = [DateTime]::MinValue)
    if ($DeadlineUtc -eq [DateTime]::MinValue) { $DeadlineUtc = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds) }
    $events = [System.Collections.Generic.List[object]]::new()
    $seenObservationIds = [System.Collections.Generic.HashSet[long]]::new()
    $after = [long]0
    $pageLimit = 10
    $maximumPages = 256
    for ($pageNumber = 1; $pageNumber -le $maximumPages; $pageNumber++) {
        $remainingMilliseconds = ($DeadlineUtc - [DateTime]::UtcNow).TotalMilliseconds
        if ($remainingMilliseconds -le 0) {
            return [pscustomobject]@{ success = $false; code = 'HOOK_SOURCE_PAGINATION_TIMEOUT'; events = @(); pages = $pageNumber - 1 }
        }
        $pageTimeoutMilliseconds = [int][Math]::Max(1, [Math]::Min(10000, [Math]::Floor($remainingMilliseconds)))
        $call = Invoke-SwarmCall -Scenario $Scenario -CredentialPath $CredentialPath -Method 'hook.source.get' `
            -Params ([ordered]@{ source_id = $SourceId; after = $after; limit = $pageLimit }) -TimeoutMilliseconds $pageTimeoutMilliseconds
        if (-not $call.success) {
            return [pscustomobject]@{ success = $false; code = $(if ($call.code) { $call.code } else { 'HOOK_SOURCE_READ_FAILED' }); events = @(); pages = $pageNumber }
        }
        $page = $call.value
        if ($page -isnot [System.Collections.IDictionary] -or -not $page.Contains('events') -or
            -not $page.Contains('after') -or -not $page.Contains('next_after_observation_id') -or
            -not $page.Contains('has_more') -or $page['events'] -isnot [System.Collections.IList] -or
            $page['has_more'] -isnot [bool]) {
            return [pscustomobject]@{ success = $false; code = 'HOOK_SOURCE_PAGE_MALFORMED'; events = @(); pages = $pageNumber }
        }
        $pageAfter = ConvertTo-StrictInt64 -Value $page['after']
        if (-not $pageAfter.valid -or $pageAfter.value -ne $after -or $page['events'].Count -gt $pageLimit) {
            return [pscustomobject]@{ success = $false; code = 'HOOK_SOURCE_CURSOR_INVALID'; events = @(); pages = $pageNumber }
        }
        $pageEvents = $page['events']
        $lastObservationId = $null
        foreach ($event in $pageEvents) {
            if ($event -isnot [System.Collections.IDictionary] -or -not $event.Contains('observation_id') -or
                -not $event.Contains('source_event_key') -or -not $event.Contains('fact') -or
                $event['fact'] -isnot [System.Collections.IDictionary]) {
                return [pscustomobject]@{ success = $false; code = 'HOOK_SOURCE_EVENT_MALFORMED'; events = @(); pages = $pageNumber }
            }
            $observation = ConvertTo-StrictInt64 -Value $event['observation_id']
            $fact = $event['fact']
            if (-not $observation.valid -or $observation.value -le $after -or
                ($null -ne $lastObservationId -and $observation.value -le $lastObservationId) -or
                -not $seenObservationIds.Add([long]$observation.value) -or
                -not $fact.Contains('event') -or -not $fact.Contains('source_id') -or -not $fact.Contains('commit_oid') -or
                -not $fact.Contains('readback_verified') -or $fact['source_id'] -cne $SourceId -or
                $fact['event'] -cne 'git.post_commit' -or
                $fact['commit_oid'] -isnot [string] -or [string]::IsNullOrWhiteSpace($fact['commit_oid']) -or
                $fact['readback_verified'] -isnot [bool] -or $fact['readback_verified'] -ne $true -or
                $event['source_event_key'] -cne ($SourceId + ':' + $fact['commit_oid'])) {
                return [pscustomobject]@{ success = $false; code = 'HOOK_SOURCE_EVENT_IDENTITY_INVALID'; events = @(); pages = $pageNumber }
            }
            $lastObservationId = [long]$observation.value
            $events.Add($event)
        }
        $hasMore = [bool]$page['has_more']
        if ($hasMore -ne ($pageEvents.Count -eq $pageLimit)) {
            return [pscustomobject]@{ success = $false; code = 'HOOK_SOURCE_PAGE_COMPLETENESS_INVALID'; events = @(); pages = $pageNumber }
        }
        if ($pageEvents.Count -eq 0) {
            if ($null -ne $page['next_after_observation_id']) {
                return [pscustomobject]@{ success = $false; code = 'HOOK_SOURCE_EMPTY_PAGE_CURSOR_INVALID'; events = @(); pages = $pageNumber }
            }
            if ($hasMore) { return [pscustomobject]@{ success = $false; code = 'HOOK_SOURCE_CURSOR_NO_PROGRESS'; events = @(); pages = $pageNumber } }
            return [pscustomobject]@{ success = $true; code = $null; events = [object[]]$events.ToArray(); pages = $pageNumber }
        }
        $nextCursor = ConvertTo-StrictInt64 -Value $page['next_after_observation_id']
        if (-not $nextCursor.valid -or $nextCursor.value -ne $lastObservationId -or $nextCursor.value -le $after) {
            return [pscustomobject]@{ success = $false; code = 'HOOK_SOURCE_CURSOR_NO_PROGRESS'; events = @(); pages = $pageNumber }
        }
        if (-not $hasMore) { return [pscustomobject]@{ success = $true; code = $null; events = [object[]]$events.ToArray(); pages = $pageNumber } }
        $after = $nextCursor.value
    }
    return [pscustomobject]@{ success = $false; code = 'HOOK_SOURCE_PAGE_LIMIT_REACHED'; events = @(); pages = $maximumPages }
}

function Test-HookObservation {
    param($Event, [Parameter(Mandatory)][string] $SourceId, [Parameter(Mandatory)][string] $CommitOid,
        [Parameter(Mandatory)][long] $ObservationId)
    if ($Event -isnot [System.Collections.IDictionary] -or -not $Event.Contains('fact') -or
        $Event['fact'] -isnot [System.Collections.IDictionary] -or -not $Event.Contains('observation_id') -or
        -not $Event.Contains('source_event_key')) { return $false }
    $observation = ConvertTo-StrictInt64 -Value $Event['observation_id']
    $fact = $Event['fact']
    return $observation.valid -and $observation.value -eq $ObservationId -and
        $fact['event'] -ceq 'git.post_commit' -and
        $fact['source_id'] -ceq $SourceId -and $fact['commit_oid'] -ceq $CommitOid -and
        $fact['readback_verified'] -is [bool] -and $fact['readback_verified'] -eq $true -and
        $Event['source_event_key'] -ceq ($SourceId + ':' + $CommitOid)
}

function Test-HookDuplicateAcknowledgement {
    param($Value, [Parameter(Mandatory)][string] $SourceId, [Parameter(Mandatory)][string] $CommitOid,
        [Parameter(Mandatory)][long] $ObservationId)
    if ($Value -isnot [System.Collections.IDictionary] -or
        -not $Value.Contains('event') -or -not $Value.Contains('source_id') -or
        -not $Value.Contains('commit_oid') -or -not $Value.Contains('readback_verified') -or
        -not $Value.Contains('duplicate') -or -not $Value.Contains('recorded') -or
        -not $Value.Contains('observation_id')) { return $false }
    $observation = ConvertTo-StrictInt64 -Value $Value['observation_id']
    return $Value['event'] -ceq 'git.post_commit' -and $Value['source_id'] -ceq $SourceId -and
        $Value['commit_oid'] -ceq $CommitOid -and $Value['readback_verified'] -is [bool] -and
        $Value['readback_verified'] -eq $true -and $Value['duplicate'] -is [bool] -and
        $Value['duplicate'] -eq $true -and $Value['recorded'] -is [bool] -and
        $Value['recorded'] -eq $false -and $observation.valid -and $observation.value -eq $ObservationId
}

function Wait-HookSourceAdmission {
    param([Parameter(Mandatory)] $Scenario, [Parameter(Mandatory)][string] $CredentialPath,
        [Parameter(Mandatory)][string] $SourceId, [Parameter(Mandatory)][string] $CommitOid)
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        $read = Read-PublicHookSourceEvents -Scenario $Scenario -CredentialPath $CredentialPath -SourceId $SourceId -DeadlineUtc $deadline
        if (-not $read.success) { return [pscustomobject]@{ success = $false; code = $read.code; observation_id = $null; event = $null } }
        if ($read.events.Count -gt 1) {
            return [pscustomobject]@{ success = $false; code = 'HOOK_PRE_RESTART_EVENT_AMBIGUOUS'; observation_id = $null; event = $null }
        }
        if ($read.events.Count -eq 1) {
            $event = $read.events[0]
            $observation = ConvertTo-StrictInt64 -Value $event['observation_id']
            if (-not $observation.valid -or -not (Test-HookObservation -Event $event -SourceId $SourceId -CommitOid $CommitOid -ObservationId $observation.value)) {
                return [pscustomobject]@{ success = $false; code = 'HOOK_PRE_RESTART_EVENT_IDENTITY_MISMATCH'; observation_id = $null; event = $null }
            }
            return [pscustomobject]@{ success = $true; code = $null; observation_id = [long]$observation.value; event = $event }
        }
        Start-Sleep -Milliseconds 300
    }
    return [pscustomobject]@{ success = $false; code = 'HOOK_PRE_RESTART_ADMISSION_NOT_OBSERVED'; observation_id = $null; event = $null }
}

function Assert-TaskSpecFixture {
    param([Parameter(Mandatory)][System.Collections.IDictionary] $Spec)
    if (-not ($Spec.objective -is [string]) -or [string]::IsNullOrWhiteSpace($Spec.objective) -or
        -not ($Spec.phase -is [string]) -or [string]::IsNullOrWhiteSpace($Spec.phase) -or
        -not ($Spec.requirements -is [System.Collections.IList]) -or $Spec.requirements.Count -lt 1 -or
        -not ($Spec.scope -is [System.Collections.IDictionary]) -or
        -not ($Spec.scope.initial_paths -is [System.Collections.IList]) -or $Spec.scope.initial_paths.Count -lt 1) {
        Stop-Harness 'TASK_SPEC_FIXTURE_INCOMPLETE'
    }
    foreach ($requirement in $Spec.requirements) {
        if (-not ($requirement -is [System.Collections.IDictionary]) -or
            [string]::IsNullOrWhiteSpace([string]$requirement.id) -or
            [string]::IsNullOrWhiteSpace([string]$requirement.statement)) { Stop-Harness 'TASK_SPEC_FIXTURE_INCOMPLETE' }
    }
    foreach ($path in $Spec.scope.initial_paths) {
        if (-not ($path -is [string]) -or [string]::IsNullOrWhiteSpace($path)) { Stop-Harness 'TASK_SPEC_FIXTURE_INCOMPLETE' }
    }
}

function Remove-OwnedCredentialFiles {
    param([Parameter(Mandatory)] $Scenario)
    foreach ($path in @((Join-Path $Scenario.path 'manager.json'), $Scenario.operator_path, $Scenario.hook_credential_path)) {
        if (Test-Path -LiteralPath $path -PathType Leaf) {
            try { Remove-Item -LiteralPath $path -Force -ErrorAction Stop } catch { }
        }
    }
}

function Get-OptionalWorkerSummary {
    param([Parameter(Mandatory)] $HostStatus)
    $workers = $HostStatus.host_lifecycle.optional_workers
    if ($null -eq $workers -or $workers -isnot [System.Collections.IDictionary]) { return @{} }
    $safe = [ordered]@{}
    foreach ($name in $workers.Keys) {
        if ([string]$name -notmatch '\A[a-z0-9-]{1,48}\z') { continue }
        $value = $workers[$name]
        $state = [string]$value.state
        if ($state -notin @('dormant', 'running', 'retry_wait', 'isolated')) { $state = 'unknown' }
        $errorCode = [string]$value.last_error_code
        if ($errorCode -notmatch '\A[A-Z0-9_]{1,64}\z') { $errorCode = $null }
        $safe[$name] = [ordered]@{ state = $state; consecutive_failures = [long]$value.consecutive_failures; last_error_code = $errorCode }
    }
    return $safe
}

function Invoke-ManagerDroppedAckScenario {
    $scenario = New-ScenarioDirectory -Name 'manager-dropped-ack-restart'
    $result = [ordered]@{ name = $scenario.name; run_id = (Split-Path -Leaf $scenario.path); status = 'blocked'; code = $null; facts = [ordered]@{} }
    try {
        $hostStart = Start-IsolatedHost $scenario
        if (-not $hostStart.ready) {
            if ($null -ne $hostStart.host_pid) { $result.facts.host_pid = [int]$hostStart.host_pid; $result.facts.host_image_sha256 = $script:HostBuild.binary_sha256 }
            $result.code = $hostStart.code
            return $result
        }
        $result.facts.host_pid = [int]$hostStart.host_pid
        $result.facts.host_image_sha256 = $script:HostBuild.binary_sha256
        $result.facts.host_epoch_before = [long]$hostStart.epoch
        $manager = New-Manager $scenario
        if (-not $manager.success) { $result.code = $manager.code; return $result }
        $taskSpec = Read-JsonFile $script:TaskSpecPath
        Assert-TaskSpecFixture $taskSpec
        $taskBaseline = Read-PublicListPages -Scenario $scenario -CredentialPath $manager.path -Method 'task.list' -IdentityField 'task_id'
        if (-not $taskBaseline.success) {
            $result.status = 'unknown'; $result.code = $taskBaseline.code; $result.facts.public_task_baseline = 'unknown'; return $result
        }
        $result.facts.public_task_count_before = $taskBaseline.items.Count
        $result.facts.public_task_list_pages_before = $taskBaseline.pages
        if ($taskBaseline.items.Count -ne 0) {
            $result.status = 'unknown'; $result.code = 'PRE_SEND_PUBLIC_TASK_BASELINE_NOT_EMPTY'; $result.facts.public_task_baseline = 'not_empty'; return $result
        }
        $operationBaseline = Read-PublicListPages -Scenario $scenario -CredentialPath $manager.path -Method 'operation.list' -IdentityField 'operation_id'
        if (-not $operationBaseline.success) {
            $result.status = 'unknown'; $result.code = $operationBaseline.code; $result.facts.task_create_operation_baseline = 'unknown'; return $result
        }
        $priorTaskCreateOperations = @($operationBaseline.items | Where-Object { $_['method'] -ceq 'task.create' })
        $result.facts.task_create_operation_count_before = $priorTaskCreateOperations.Count
        $result.facts.operation_list_pages_before = $operationBaseline.pages
        if ($priorTaskCreateOperations.Count -ne 0) {
            $result.status = 'unknown'; $result.code = 'PRE_SEND_TASK_CREATE_OPERATION_BASELINE_NOT_EMPTY'; $result.facts.task_create_operation_baseline = 'not_empty'; return $result
        }
        $result.facts.public_task_baseline = 'empty'
        $result.facts.task_create_operation_baseline = 'empty'
        $origin = 'core-failure:' + [Guid]::NewGuid().ToString('N')
        $logicalId = 'core-failure:' + [Guid]::NewGuid().ToString('N')
        $params = [ordered]@{ client_request_id = $logicalId; project_id = $ProjectId; origin_key = $origin; spec = $taskSpec }
        $send = Invoke-DroppedReplyRpc -DataDirectory $scenario.state -PipeName $scenario.pipe_name -Credential $manager.credential -Method 'task.create' -Params $params
        $result.facts.rpc_transport_code = [string]$send.code
        $result.facts.application_effect_possible = [bool]$send.effect_possible
        if (-not $send.sent) { $result.code = $send.code; return $result }
        if ($send.code -cne 'ACK_DROPPED_BY_HARNESS' -or -not $send.effect_possible) {
            $result.status = 'unknown'
            $result.code = $(if ($send.code) { $send.code } else { 'RPC_OUTCOME_UNKNOWN' })
            $result.facts.manager_ack = 'unknown_after_possible_write'
            return $result
        }
        $result.facts.manager_ack = 'intentionally_not_read'
        $result.facts.logical_request_id = $logicalId
        $admitted = Read-TaskCreateAdmission -Scenario $scenario -Manager $manager -ExpectedProjectId $ProjectId -ExpectedSpec $taskSpec
        if (-not $admitted.success) {
            $result.status = 'unknown'
            $result.code = $(if ($admitted.code) { $admitted.code } else { 'PRE_RESTART_ADMISSION_READBACK_MISMATCH' })
            $result.facts.public_task_identity_before_restart = 'unknown'
            $result.facts.public_task_identity_before_restart_code = $result.code
            return $result
        }
        $admittedOperationId = [string]$admitted.operation_id
        $admittedTaskId = [string]$admitted.task_id
        $result.facts.admitted_operation_before_restart = $admittedOperationId
        $result.facts.admitted_task_before_restart = $admittedTaskId
        $result.facts.task_create_operation_list_pages_after_send = $admitted.pages
        $result.facts.public_task_identity_before_restart = 'verified'
        $stopped = Stop-IsolatedHost $scenario
        if (-not $stopped.stopped) { $result.status = 'pending'; $result.code = 'OWN_HOST_STOP_PENDING'; $result.facts.pending_host_pid = $stopped.pending_host_pid; return $result }
        $restarted = Start-IsolatedHost $scenario
        if (-not $restarted.ready) {
            if ($null -ne $restarted.host_pid) { $result.facts.host_restart_pid = [int]$restarted.host_pid; $result.facts.host_restart_image_sha256 = $script:HostBuild.binary_sha256 }
            $result.status = 'unknown'; $result.code = $restarted.code; return $result
        }
        $result.facts.host_restart_pid = [int]$restarted.host_pid
        $result.facts.host_restart_image_sha256 = $script:HostBuild.binary_sha256
        $result.facts.host_epoch_after = [long]$restarted.epoch
        if ($restarted.epoch -le $hostStart.epoch) { $result.status = 'unknown'; $result.code = 'HOST_EPOCH_DID_NOT_ADVANCE'; return $result }
        $operationAfterRestart = Invoke-SwarmCall -Scenario $scenario -CredentialPath $manager.path -Method 'operation.get' `
            -Params ([ordered]@{ operation_id = $admittedOperationId })
        if (-not $operationAfterRestart.success) {
            $result.status = 'unknown'
            $result.code = $(if ($operationAfterRestart.code) { $operationAfterRestart.code } else { 'POST_RESTART_OPERATION_READ_FAILED' })
            $result.facts.public_task_identity_after_restart = 'unknown'
            $result.facts.public_task_identity_after_restart_code = $result.code
            return $result
        }
        $taskAfterRestart = Invoke-SwarmCall -Scenario $scenario -CredentialPath $manager.path -Method 'task.get' `
            -Params ([ordered]@{ task_id = $admittedTaskId })
        if (-not $taskAfterRestart.success) {
            $result.status = 'unknown'
            $result.code = $(if ($taskAfterRestart.code) { $taskAfterRestart.code } else { 'POST_RESTART_TASK_READ_FAILED' })
            $result.facts.public_task_identity_after_restart = 'unknown'
            return $result
        }
        $operationValueAfterRestart = $operationAfterRestart.value
        $taskValueAfterRestart = $taskAfterRestart.value
        if ($operationValueAfterRestart -isnot [System.Collections.IDictionary] -or
            -not $operationValueAfterRestart.Contains('operation_id') -or -not $operationValueAfterRestart.Contains('method') -or
            -not $operationValueAfterRestart.Contains('state') -or -not $operationValueAfterRestart.Contains('task_id') -or
            -not $operationValueAfterRestart.Contains('result') -or $operationValueAfterRestart['result'] -isnot [System.Collections.IDictionary] -or
            $operationValueAfterRestart['operation_id'] -cne $admittedOperationId -or
            $operationValueAfterRestart['method'] -cne 'task.create' -or $operationValueAfterRestart['state'] -cne 'settled' -or
            $operationValueAfterRestart['task_id'] -cne $admittedTaskId -or
            $operationValueAfterRestart['result']['task_id'] -cne $admittedTaskId -or
            -not (Test-PublicTaskMatchesFixture -Task $taskValueAfterRestart -TaskId $admittedTaskId -ExpectedProjectId $ProjectId -ExpectedSpec $taskSpec)) {
            $result.status = 'unknown'; $result.code = 'POST_RESTART_TASK_CREATE_RECEIPT_OR_TASK_MISMATCH'; return $result
        }
        if (-not (Test-JsonSemanticEqual -Left $admitted.operation -Right $operationValueAfterRestart) -or
            -not (Test-JsonSemanticEqual -Left $admitted.task -Right $taskValueAfterRestart)) {
            $result.status = 'unknown'; $result.code = 'POST_RESTART_PUBLIC_RECEIPT_OR_TASK_CHANGED'; return $result
        }
        $result.facts.public_task_identity_after_restart = 'verified'
        $result.facts.operation_id_after_restart = $admittedOperationId
        $result.facts.task_id_after_restart = $admittedTaskId
        $result.facts.operation_id = $admittedOperationId
        $result.facts.task_id = $admittedTaskId
        $result.status = 'observed'
        $result.facts.manager_readback_after_restart = $true
        $result.facts.operation_state_after_restart = [string]$operationValueAfterRestart['state']
        $result.facts.public_receipt_stable_across_restart = $true
        $result.facts.public_task_stable_across_restart = $true
        $result.facts.replayed = $false
        $result.facts.effect = 'one local Task create; caller acknowledgement was unknown until exact post-restart Manager readback'
        return $result
    }
    finally {
        $stopped = Stop-IsolatedHost $scenario
        if (-not $stopped.stopped) { $result.facts.pending_host_pid = $stopped.pending_host_pid } else { Remove-OwnedCredentialFiles $scenario }
        Add-Scenario $result
    }
}

function Invoke-ManagerConflictScenario {
    $scenario = New-ScenarioDirectory -Name 'manager-durable-request-conflict'
    $result = [ordered]@{ name = $scenario.name; run_id = (Split-Path -Leaf $scenario.path); status = 'blocked'; code = $null; facts = [ordered]@{} }
    try {
        $hostStart = Start-IsolatedHost $scenario
        if (-not $hostStart.ready) {
            if ($null -ne $hostStart.host_pid) { $result.facts.host_pid = [int]$hostStart.host_pid; $result.facts.host_image_sha256 = $script:HostBuild.binary_sha256 }
            $result.code = $hostStart.code
            return $result
        }
        $result.facts.host_pid = [int]$hostStart.host_pid
        $result.facts.host_image_sha256 = $script:HostBuild.binary_sha256
        $manager = New-Manager $scenario
        if (-not $manager.success) { $result.code = $manager.code; return $result }
        $taskSpec = Read-JsonFile $script:TaskSpecPath
        Assert-TaskSpecFixture $taskSpec
        $requestId = 'core-failure:' + [Guid]::NewGuid().ToString('N')
        $origin = 'core-failure:' + [Guid]::NewGuid().ToString('N')
        $first = Invoke-SwarmCall -Scenario $scenario -CredentialPath $manager.path -Method 'task.create' -Params ([ordered]@{ client_request_id = $requestId; project_id = $ProjectId; origin_key = $origin; spec = $taskSpec })
        if (-not $first.success -or [string]::IsNullOrWhiteSpace([string]$first.value.task_id)) { $result.code = $(if ($first.code) { $first.code } else { 'TASK_CREATE_READBACK_REQUIRED' }); return $result }
        $originalTaskId = [string]$first.value.task_id
        $operations = Invoke-SwarmCall -Scenario $scenario -CredentialPath $manager.path -Method 'operation.get' -Params ([ordered]@{ operation_id = [string]$first.value.operation_id })
        if (-not $operations.success -or $operations.value.state -cne 'settled') { $result.status = 'unknown'; $result.code = 'ORIGINAL_OPERATION_READBACK_FAILED'; return $result }
        $conflictOrigin = 'core-failure:' + [Guid]::NewGuid().ToString('N')
        $second = Invoke-SwarmCall -Scenario $scenario -CredentialPath $manager.path -Method 'task.create' -Params ([ordered]@{ client_request_id = $requestId; project_id = $ProjectId; origin_key = $conflictOrigin; spec = $taskSpec })
        if ($second.success -or $second.code -cne 'REQUEST_ID_CONFLICT') { $result.status = 'unknown'; $result.code = $(if ($second.code) { $second.code } else { 'REQUEST_CONFLICT_NOT_REJECTED' }); return $result }
        $stopped = Stop-IsolatedHost $scenario
        if (-not $stopped.stopped) { $result.status = 'pending'; $result.code = 'OWN_HOST_STOP_PENDING'; $result.facts.pending_host_pid = $stopped.pending_host_pid; return $result }
        $restarted = Start-IsolatedHost $scenario
        if (-not $restarted.ready) {
            if ($null -ne $restarted.host_pid) { $result.facts.host_restart_pid = [int]$restarted.host_pid; $result.facts.host_restart_image_sha256 = $script:HostBuild.binary_sha256 }
            $result.status = 'unknown'; $result.code = $restarted.code; return $result
        }
        $result.facts.host_restart_pid = [int]$restarted.host_pid
        $result.facts.host_restart_image_sha256 = $script:HostBuild.binary_sha256
        $result.facts.host_epoch_after = [long]$restarted.epoch
        $after = Invoke-SwarmCall -Scenario $scenario -CredentialPath $manager.path -Method 'operation.get' -Params ([ordered]@{ operation_id = [string]$operations.value.operation_id })
        $tasks = Invoke-SwarmCall -Scenario $scenario -CredentialPath $manager.path -Method 'task.list' -Params ([ordered]@{ after = 0; limit = 50 })
        $retainedTasks = @(if ($tasks.success) { $tasks.value.items })
        if (-not $after.success -or $after.value.operation_id -cne $operations.value.operation_id -or $after.value.result.task_id -cne $originalTaskId -or
            -not $tasks.success -or $tasks.value.coverage_complete -ne $true -or $tasks.value.has_newer -ne $false -or
            $retainedTasks.Count -ne 1 -or $retainedTasks[0].task_id -cne $originalTaskId) {
            $result.status = 'unknown'; $result.code = 'DURABLE_CONFLICT_READBACK_MISMATCH'; return $result
        }
        $result.status = 'observed'
        $result.facts.manager_error_code = 'REQUEST_ID_CONFLICT'
        $result.facts.original_operation_id = [string]$after.value.operation_id
        $result.facts.original_task_id = $originalTaskId
        $result.facts.original_operation_state = [string]$after.value.state
        $result.facts.conflicting_task_created = $false
        $result.facts.complete_public_task_identity_set_verified = $true
        $result.facts.current_manager_handover = $true
        $result.facts.gm_epoch = $manager.gm_epoch
        return $result
    }
    finally {
        $stopped = Stop-IsolatedHost $scenario
        if (-not $stopped.stopped) { $result.facts.pending_host_pid = $stopped.pending_host_pid } else { Remove-OwnedCredentialFiles $scenario }
        Add-Scenario $result
    }
}

function Invoke-HookRestartDedupScenario {
    $scenario = New-ScenarioDirectory -Name 'hook-callback-restart-dedup'
    $result = [ordered]@{ name = $scenario.name; run_id = (Split-Path -Leaf $scenario.path); status = 'blocked'; code = $null; facts = [ordered]@{} }
    $sourceId = $null
    $sourceRevision = $null
    try {
        if ([string]::IsNullOrWhiteSpace($HookProjectId) -or [string]::IsNullOrWhiteSpace($HookCommitOid)) { $result.code = 'HOOK_FIXTURE_INPUTS_REQUIRED'; return $result }
        $hostStart = Start-IsolatedHost $scenario
        if (-not $hostStart.ready) {
            if ($null -ne $hostStart.host_pid) { $result.facts.host_pid = [int]$hostStart.host_pid; $result.facts.host_image_sha256 = $script:HostBuild.binary_sha256 }
            $result.code = $hostStart.code
            return $result
        }
        $result.facts.host_pid = [int]$hostStart.host_pid
        $result.facts.host_image_sha256 = $script:HostBuild.binary_sha256
        $baseArgs = @('--config', $script:ConfigPath, '--data-dir', $scenario.state, '--credential', $scenario.operator_path, 'hook')
        $previewResult = Start-BoundedProcess -Arguments ($baseArgs + @('install', 'preview', $HookProjectId)) -TimeoutMilliseconds 15000
        if (-not $previewResult.started -or -not $previewResult.completed -or $previewResult.exit_code -ne 0) { $result.code = $previewResult.error_code; return $result }
        $preview = $previewResult.stdout | ConvertFrom-Json -AsHashtable -Depth 16
        $gitDirectory = Assert-SafeAbsolutePath -Path ([string]$preview.git_directory) -MustExist -Directory
        $setupResult = Start-BoundedProcess -Arguments ($baseArgs + @('setup', $HookProjectId)) -TimeoutMilliseconds 30000
        if (-not $setupResult.started -or -not $setupResult.completed -or $setupResult.exit_code -ne 0) { $result.code = $setupResult.error_code; return $result }
        $setupValue = $setupResult.stdout | ConvertFrom-Json -AsHashtable -Depth 24
        $sourceId = [Guid]::Parse([string]$setupValue.source.source_id).ToString('D')
        $sourceRevision = [long]$setupValue.source.revision
        if ($setupValue.credential_file_written -ne $true -or $setupValue.installation.wrapper_matches -ne $true -or $setupValue.installation.state -cne 'installed') { $result.code = 'HOOK_SETUP_READBACK_MISMATCH'; return $result }
        $credentialPath = Assert-SafeAbsolutePath -Path (Join-Path $gitDirectory ('eliot-hook-sources\' + $sourceId + '\credential.json')) -MustExist
        $hookCredential = Read-JsonFile $credentialPath
        if ($hookCredential.client_id -cne ('hook-source:' + $sourceId)) { $result.code = 'HOOK_CREDENTIAL_IDENTITY_MISMATCH'; return $result }
        Write-PrivateJson -Path $scenario.hook_credential_path -Value $hookCredential
        $eventParams = [ordered]@{ source_id = $sourceId; commit_oid = $HookCommitOid.ToLowerInvariant() }
        $sourceBaseline = Read-PublicHookSourceEvents -Scenario $scenario -CredentialPath $scenario.hook_credential_path -SourceId $sourceId
        if (-not $sourceBaseline.success) {
            $result.status = 'unknown'; $result.code = $sourceBaseline.code; $result.facts.hook_source_baseline = 'unknown'; return $result
        }
        $result.facts.hook_source_event_count_before = $sourceBaseline.events.Count
        $result.facts.hook_source_baseline_pages = $sourceBaseline.pages
        if ($sourceBaseline.events.Count -ne 0) {
            $result.status = 'unknown'; $result.code = 'HOOK_SOURCE_BASELINE_NOT_EMPTY'; $result.facts.hook_source_baseline = 'not_empty'; return $result
        }
        $result.facts.hook_source_baseline = 'empty_for_setup_issued_source'
        $dropped = Invoke-DroppedReplyRpc -DataDirectory $scenario.state -PipeName $scenario.pipe_name -Credential $hookCredential -Method 'hook.emit' -Params $eventParams
        $result.facts.rpc_transport_code = [string]$dropped.code
        $result.facts.application_effect_possible = [bool]$dropped.effect_possible
        if (-not $dropped.sent) { $result.code = $dropped.code; return $result }
        if ($dropped.code -cne 'ACK_DROPPED_BY_HARNESS' -or -not $dropped.effect_possible) {
            $result.status = 'unknown'
            $result.code = $(if ($dropped.code) { $dropped.code } else { 'RPC_OUTCOME_UNKNOWN' })
            $result.facts.callback_ack = 'unknown_after_possible_write'
            return $result
        }
        $result.facts.callback_ack = 'intentionally_not_read'
        $result.facts.source_id = $sourceId
        $result.facts.commit_oid = $HookCommitOid.ToLowerInvariant()
        $preRestartAdmission = Wait-HookSourceAdmission -Scenario $scenario -CredentialPath $scenario.hook_credential_path `
            -SourceId $sourceId -CommitOid $eventParams.commit_oid
        if (-not $preRestartAdmission.success) {
            $result.status = 'unknown'
            $result.code = $preRestartAdmission.code
            $result.facts.verified_event_before_restart = 'unknown'
            $result.facts.verified_event_before_restart_code = $preRestartAdmission.code
            return $result
        }
        $firstId = [long]$preRestartAdmission.observation_id
        $result.facts.observation_id_before_restart = $firstId
        $result.facts.verified_event_before_restart = 'verified'
        $stopped = Stop-IsolatedHost $scenario
        if (-not $stopped.stopped) { $result.status = 'pending'; $result.code = 'OWN_HOST_STOP_PENDING'; $result.facts.pending_host_pid = $stopped.pending_host_pid; return $result }
        $restarted = Start-IsolatedHost $scenario
        if (-not $restarted.ready) {
            if ($null -ne $restarted.host_pid) { $result.facts.host_restart_pid = [int]$restarted.host_pid; $result.facts.host_restart_image_sha256 = $script:HostBuild.binary_sha256 }
            $result.status = 'unknown'; $result.code = $restarted.code; return $result
        }
        $result.facts.host_restart_pid = [int]$restarted.host_pid
        $result.facts.host_restart_image_sha256 = $script:HostBuild.binary_sha256
        $result.facts.host_epoch_after = [long]$restarted.epoch
        $read = Read-PublicHookSourceEvents -Scenario $scenario -CredentialPath $scenario.hook_credential_path -SourceId $sourceId
        if (-not $read.success) { $result.status = 'unknown'; $result.code = $(if ($read.code) { $read.code } else { 'HOOK_READBACK_FAILED' }); return $result }
        $events = @($read.events)
        if ($events.Count -ne 1 -or -not (Test-HookObservation -Event $events[0] -SourceId $sourceId `
            -CommitOid $eventParams.commit_oid -ObservationId $firstId)) {
            $result.status = 'unknown'; $result.code = 'HOOK_EVENT_NOT_UNIQUE_AFTER_RESTART'; return $result
        }
        $result.facts.observation_id_after_restart = [long]$events[0].observation_id
        $duplicate = Invoke-SwarmCall -Scenario $scenario -CredentialPath $scenario.hook_credential_path -Method 'hook.emit' -Params $eventParams
        if (-not $duplicate.success -or -not (Test-HookDuplicateAcknowledgement -Value $duplicate.value `
            -SourceId $sourceId -CommitOid $eventParams.commit_oid -ObservationId $firstId)) {
            $result.status = 'unknown'; $result.code = $(if ($duplicate.code) { $duplicate.code } else { 'HOOK_DUPLICATE_ACK_MISMATCH' }); return $result
        }
        $finalRead = Read-PublicHookSourceEvents -Scenario $scenario -CredentialPath $scenario.hook_credential_path -SourceId $sourceId
        $finalEvents = @()
        if ($finalRead.success) { $finalEvents = @($finalRead.events) }
        if (-not $finalRead.success -or $finalEvents.Count -ne 1 -or
            -not (Test-HookObservation -Event $finalEvents[0] -SourceId $sourceId -CommitOid $eventParams.commit_oid -ObservationId $firstId)) {
            $result.status = 'unknown'; $result.code = 'HOOK_DEDUP_READBACK_MISMATCH'; return $result
        }
        $result.status = 'observed'
        $result.facts.observation_id = $firstId
        $result.facts.duplicate_callback = $true
        $result.facts.durable_after_host_restart = $true
        $result.facts.callback_identity = '{source_id, commit_oid}'
        $result.facts.replayed_native_effect = $false
        return $result
    }
    finally {
        if ($null -ne $sourceId -and $null -ne $scenario.host -and -not $scenario.host.HasExited) {
            $revokeArgs = @('--config', $script:ConfigPath, '--data-dir', $scenario.state, '--credential', $scenario.operator_path, 'hook', 'install', 'revoke', $HookProjectId, $sourceId, '--revision', [string]$sourceRevision)
            $revocation = Start-BoundedProcess -Arguments $revokeArgs -TimeoutMilliseconds 30000
            if (-not $revocation.started -or -not $revocation.completed -or $revocation.exit_code -ne 0) {
                $result.status = 'pending'; $result.code = 'HOOK_INSTALL_CLEANUP_PENDING'
            } else { $result.facts.fixture_hook_revoked_and_restored = $true }
        }
        $stopped = Stop-IsolatedHost $scenario
        if (-not $stopped.stopped) { $result.facts.pending_host_pid = $stopped.pending_host_pid } else { Remove-OwnedCredentialFiles $scenario }
        Add-Scenario $result
    }
}

function Invoke-OptionalWorkerObservation {
    $scenario = New-ScenarioDirectory -Name 'optional-worker-observation'
    $result = [ordered]@{ name = $scenario.name; run_id = (Split-Path -Leaf $scenario.path); status = 'blocked'; code = $null; facts = [ordered]@{} }
    try {
        $hostStart = Start-IsolatedHost $scenario
        if (-not $hostStart.ready) {
            if ($null -ne $hostStart.host_pid) { $result.facts.host_pid = [int]$hostStart.host_pid; $result.facts.host_image_sha256 = $script:HostBuild.binary_sha256 }
            $result.code = $hostStart.code
            return $result
        }
        $result.facts.host_pid = [int]$hostStart.host_pid
        $result.facts.host_image_sha256 = $script:HostBuild.binary_sha256
        $manager = New-Manager $scenario
        if (-not $manager.success) { $result.code = $manager.code; return $result }
        $status = Invoke-SwarmCall -Scenario $scenario -CredentialPath $manager.path -Method 'host.status' -Params ([ordered]@{})
        if (-not $status.success) { $result.code = $(if ($status.code) { $status.code } else { 'HOST_STATUS_READBACK_FAILED' }); return $result }
        $workers = Get-OptionalWorkerSummary $status.value
        $result.facts.manager_visible_workers = $workers
        $failed = @($workers.GetEnumerator() | Where-Object { $_.Value.state -in @('retry_wait', 'isolated') })
        if ($failed.Count -gt 0) {
            $result.status = 'observed'
            $result.facts.failure_was_injected_by_harness = $false
            $result.facts.interpretation = 'existing bounded health is Manager-visible; this harness has no public control that can induce a specific legacy worker failure'
            return $result
        }
        $result.status = 'blocked'
        $result.code = 'NO_PUBLIC_OPTIONAL_WORKER_FAULT_TRIGGER'
        $result.facts.healthy_or_dormant_status_observed = $true
        $result.facts.failure_was_injected_by_harness = $false
        return $result
    }
    finally {
        $stopped = Stop-IsolatedHost $scenario
        if (-not $stopped.stopped) { $result.facts.pending_host_pid = $stopped.pending_host_pid } else { Remove-OwnedCredentialFiles $scenario }
        Add-Scenario $result
    }
}

try {
    if (-not $IsWindows -or $PSVersionTable.PSVersion.Major -lt 7) { Stop-Harness 'POWERSHELL_7_WINDOWS_REQUIRED' }
    $script:HostPath = Assert-SafeAbsolutePath -Path $HostExecutable -MustExist
    $script:HostSupervisorPath = Assert-SafeAbsolutePath -Path $HostSupervisorExecutable -MustExist
    $script:HostLauncherPath = Assert-SafeAbsolutePath -Path $HostLauncherExecutable -MustExist
    $script:PublicCliPath = Assert-SafeAbsolutePath -Path $PublicCliExecutable -MustExist
    $actualHostHash = Get-Sha256 $script:HostPath
    $actualHostSupervisorHash = Get-Sha256 $script:HostSupervisorPath
    $actualHostLauncherHash = Get-Sha256 $script:HostLauncherPath
    $actualPublicCliHash = Get-Sha256 $script:PublicCliPath
    if ($actualHostHash -cne $ExpectedHostSha256.ToLowerInvariant()) { Stop-Harness 'HOST_BINARY_HASH_MISMATCH' }
    if ($actualHostSupervisorHash -cne $ExpectedHostSupervisorSha256.ToLowerInvariant()) { Stop-Harness 'HOST_SUPERVISOR_BINARY_HASH_MISMATCH' }
    if ($actualHostLauncherHash -cne $ExpectedHostLauncherSha256.ToLowerInvariant()) { Stop-Harness 'HOST_LAUNCHER_BINARY_HASH_MISMATCH' }
    if ($actualPublicCliHash -cne $ExpectedPublicCliSha256.ToLowerInvariant()) { Stop-Harness 'PUBLIC_CLI_BINARY_HASH_MISMATCH' }
    $script:HostBuild = Get-PinnedBuildProvenance -ManifestPath $HostBuildManifestPath -ExpectedManifestSha256 $ExpectedHostBuildManifestSha256 -BinaryPath $script:HostPath -ExpectedBinarySha256 $actualHostHash -ExpectedPackage 'swarm-kernel-host' -ExpectedTarget 'swarm-kernel-host'
    $script:HostSupervisorBuild = Get-PinnedBuildProvenance -ManifestPath $HostSupervisorBuildManifestPath -ExpectedManifestSha256 $ExpectedHostSupervisorBuildManifestSha256 -BinaryPath $script:HostSupervisorPath -ExpectedBinarySha256 $actualHostSupervisorHash -ExpectedPackage 'swarm-supervisor' -ExpectedTarget 'swarm-supervisor'
    $script:HostLauncherBuild = Get-PinnedBuildProvenance -ManifestPath $HostLauncherBuildManifestPath -ExpectedManifestSha256 $ExpectedHostLauncherBuildManifestSha256 -BinaryPath $script:HostLauncherPath -ExpectedBinarySha256 $actualHostLauncherHash -ExpectedPackage 'eliot-swarm-controller' -ExpectedTarget 'swarm-host'
    $script:PublicCliBuild = Get-PinnedBuildProvenance -ManifestPath $PublicCliBuildManifestPath -ExpectedManifestSha256 $ExpectedPublicCliBuildManifestSha256 -BinaryPath $script:PublicCliPath -ExpectedBinarySha256 $actualPublicCliHash -ExpectedPackage 'swarm-cli' -ExpectedTarget 'swarm' -FrontendCli
    Assert-PublicCliHostSibling -PublicCliBuild $script:PublicCliBuild -HostLauncherBuild $script:HostLauncherBuild -HostBuild $script:HostBuild -HostSupervisorBuild $script:HostSupervisorBuild
    $script:ConfigPath = Assert-SafeAbsolutePath -Path $HostConfigPath -MustExist
    $output = Assert-SafeAbsolutePath -Path $OutputRoot -MustExist -Directory
    $script:TaskSpecPath = Assert-SafeAbsolutePath -Path $TaskSpecPath -MustExist
    $script:TaskSpecHash = Get-Sha256 $script:TaskSpecPath
    $runDirectory = Join-Path $output ([Guid]::NewGuid().ToString('D'))
    New-PrivateDirectory -Path $runDirectory
    $script:RunDirectory = $runDirectory
    $script:Summary.status = 'running'
    $script:Summary.host = [ordered]@{
        package = 'swarm-kernel-host'
        target = 'swarm-kernel-host'
        profile = 'release'
        image_sha256 = $actualHostHash
        manifest_sha256 = $script:HostBuild.manifest_sha256
        source_commit = $script:HostBuild.source_commit
        source_tree = $script:HostBuild.source_tree
        contract_base_commit = $script:ContractBaseCommit
        host_config_sha256 = Get-Sha256 $script:ConfigPath
        task_spec_sha256 = $script:TaskSpecHash
        target_dir_sha256 = $script:HostBuild.target_dir_sha256
        manifest_is_unsigned = $true
    }
    $script:Summary.host_supervisor = [ordered]@{
        package = 'swarm-supervisor'
        target = 'swarm-supervisor'
        image_sha256 = $actualHostSupervisorHash
        manifest_sha256 = $script:HostSupervisorBuild.manifest_sha256
        source_commit = $script:HostSupervisorBuild.source_commit
        source_tree = $script:HostSupervisorBuild.source_tree
        manifest_is_unsigned = $true
    }
    $script:Summary.host_launcher = [ordered]@{
        package = 'eliot-swarm-controller'
        target = 'swarm-host'
        image_sha256 = $actualHostLauncherHash
        manifest_sha256 = $script:HostLauncherBuild.manifest_sha256
        source_commit = $script:HostLauncherBuild.source_commit
        source_tree = $script:HostLauncherBuild.source_tree
        manifest_is_unsigned = $true
    }
    $script:Summary.public_cli = [ordered]@{
        package = 'swarm-cli'
        target = 'swarm'
        image_sha256 = $actualPublicCliHash
        manifest_sha256 = $script:PublicCliBuild.manifest_sha256
        source_commit = $script:PublicCliBuild.source_commit
        source_tree = $script:PublicCliBuild.source_tree
        target_dir_sha256 = $script:PublicCliBuild.target_dir_sha256
        host_launcher_package = $script:PublicCliBuild.host_launcher_package_name
        host_launcher_target = $script:PublicCliBuild.host_launcher_binary_target
        host_runtime_package = $script:PublicCliBuild.host_runtime_package_name
        host_runtime_target = $script:PublicCliBuild.host_runtime_binary_target
        host_supervisor_package = $script:PublicCliBuild.host_supervisor_package_name
        host_supervisor_target = $script:PublicCliBuild.host_supervisor_binary_target
        host_ipc_protocol_version = $script:PublicCliBuild.host_ipc_protocol_version
        exact_host_sibling_verified = $true
        manifest_is_unsigned = $true
    }
    Save-Summary

    $null = Invoke-ManagerDroppedAckScenario
    $null = Invoke-ManagerConflictScenario
    $null = Invoke-HookRestartDedupScenario
    $null = Invoke-OptionalWorkerObservation

    $script:Summary.status = 'completed'
    $script:Summary.completed_at_utc = [DateTime]::UtcNow.ToString('o')
    if ($script:PendingHostPids.Count -gt 0) { $script:Summary.limits.pending_owned_host_process_ids = @($script:PendingHostPids.ToArray()) }
    if ($script:PendingPublicCliPids.Count -gt 0) { $script:Summary.limits.pending_public_cli_process_ids = @($script:PendingPublicCliPids.ToArray()) }
    Save-Summary
    Write-Output (Join-Path $script:RunDirectory 'qualification-receipt.json')
}
catch {
    $failure = $_
    $code = $_.Exception.Message
    if ($code -notmatch '\A[A-Z0-9_]{1,64}\z') { $code = 'HARNESS_PREFLIGHT_FAILED' }
    $script:Summary.status = 'blocked'
    $script:Summary.preflight_error_code = $code
    if ($script:RunDirectory) {
        Write-PrivateJson -Path (Join-Path $script:RunDirectory 'harness-error.json') -Value ([ordered]@{ message = $failure.Exception.Message; position = $failure.InvocationInfo.PositionMessage; stack = $failure.ScriptStackTrace })
        Save-Summary
    }
    Write-Error $code
    exit 2
}
